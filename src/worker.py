from datetime import datetime, timedelta, timezone
import json
import logging
import os
import time
from typing import Any

from google.cloud import pubsub_v1
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import Session, sessionmaker

from src.config import Configuracoes
from src.mensageria.consumidor import BillingConsumidor
from src.mensageria.outbox_inbox import OutboxEventoModel
from src.pagamentos.infraestrutura.mercadopago import (
    FakeMercadoPagoOrdersProvider,
    MercadoPagoOrdersClient,
    ProvedorPagamento,
)
from src.shared.observabilidade import configurar_observabilidade

logger = logging.getLogger("oficina-billing-worker")


def criar_provedor_pagamento(config: Configuracoes) -> ProvedorPagamento:
    if config.mp_provider_mode == "real":
        return MercadoPagoOrdersClient(
            access_token=config.mp_access_token,
            external_pos_id=config.mp_external_pos_id,
            base_url=config.mp_base_url,
        )
    return FakeMercadoPagoOrdersProvider(
        webhook_secret=config.mp_webhook_secret,
        state_file=config.mp_fake_state_file or None,
    )


def publicar_lote_outbox(
    session: Session,
    publisher: Any,
    *,
    project_id: str,
    lease_seconds: int = 30,
    limit: int = 50,
    agora: datetime | None = None,
) -> int:
    ref = agora or datetime.now(timezone.utc)
    eventos = (
        session.query(OutboxEventoModel)
        .filter(
            OutboxEventoModel.status.in_(["PENDENTE", "FALHA_AGENDADA"]),
            OutboxEventoModel.next_attempt_at <= ref,
            or_(
                OutboxEventoModel.locked_until.is_(None),
                OutboxEventoModel.locked_until <= ref,
            ),
        )
        .order_by(OutboxEventoModel.criado_em.asc())
        .limit(limit)
        .all()
    )
    if not eventos:
        return 0

    lease_expira = ref + timedelta(seconds=lease_seconds)
    for ev in eventos:
        ev.locked_until = lease_expira
    session.commit()

    publicados = 0
    for ev in eventos:
        try:
            topic_path = publisher.topic_path(project_id, ev.topic)
            data = json.dumps(ev.payload).encode("utf-8")
            attrs = {
                "event_type": ev.tipo_evento,
                "producer": "oficina-billing",
                "schema_version": "1",
            }
            if ev.traceparent:
                attrs["traceparent"] = ev.traceparent
            publisher.publish(topic_path, data, **attrs).result(timeout=10)
            ev.status = "PROCESSADO"
            ev.processado_em = datetime.now(timezone.utc)
            ev.locked_until = None
            ev.erro = None
            publicados += 1
        except Exception as exc:  # noqa: BLE001
            ev.tentativas = int(ev.tentativas or 0) + 1
            atraso = min(60, 2 ** min(ev.tentativas, 5))
            ev.status = "FALHA_AGENDADA"
            ev.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=atraso)
            ev.locked_until = None
            ev.erro = str(exc)
            logger.error("Erro ao publicar outbox billing %s: %s", ev.id, exc)
        session.commit()
    return publicados


def consumir_comandos_pull(
    session_factory: sessionmaker,
    subscriber: Any,
    provedor: ProvedorPagamento,
    *,
    project_id: str,
    subscription_name: str,
    environment: str = "homolog",
    max_messages: int = 20,
) -> int:
    sub_path = subscriber.subscription_path(project_id, subscription_name)
    resp = subscriber.pull(
        request={"subscription": sub_path, "max_messages": max_messages},
        timeout=5,
    )
    if not resp.received_messages:
        return 0

    processadas = 0
    for rec in resp.received_messages:
        ack_id = rec.ack_id
        try:
            dados = json.loads(rec.message.data.decode("utf-8"))
            traceparent = (rec.message.attributes or {}).get("traceparent")
            with session_factory() as db:
                BillingConsumidor(
                    db, provedor, environment=environment
                ).processar_comando(dados, traceparent=traceparent)
            subscriber.acknowledge(
                request={"subscription": sub_path, "ack_ids": [ack_id]}
            )
            processadas += 1
        except Exception as exc:  # noqa: BLE001
            logger.error("Erro ao processar comando no Billing: %s", exc)
            try:
                subscriber.modify_ack_deadline(
                    request={
                        "subscription": sub_path,
                        "ack_ids": [ack_id],
                        "ack_deadline_seconds": 0,
                    }
                )
            except Exception:  # noqa: BLE001
                pass
    return processadas


def main() -> None:
    cfg = Configuracoes()
    configurar_observabilidade(
        service="oficina-billing-worker",
        env=cfg.dd_env,
        version=cfg.dd_version,
        trace_enabled=cfg.dd_trace_enabled,
    )
    project_id = cfg.gcp_project_id
    env = cfg.environment
    sub_name = os.getenv("BILLING_COMANDOS_SUB", f"oficina-{env}-billing-comandos-sub")
    engine = create_engine(cfg.database_url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    provedor = criar_provedor_pagamento(cfg)
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    logger.info("Worker Billing iniciado (env=%s, sub=%s)", env, sub_name)
    while True:
        try:
            with session_factory() as db:
                publicar_lote_outbox(db, publisher, project_id=project_id)
            consumir_comandos_pull(
                session_factory,
                subscriber,
                provedor,
                project_id=project_id,
                subscription_name=sub_name,
                environment=env,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Erro no loop do worker Billing: %s", exc)
        time.sleep(0.5)


if __name__ == "__main__":
    main()
