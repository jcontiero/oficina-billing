from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import Configuracoes
from src.main import criar_app
from src.mensageria.consumidor import BillingConsumidor
from src.mensageria.outbox_inbox import OutboxEventoModel
from src.orcamentos.infraestrutura.modelos import OrcamentoModel, PagamentoModel
from src.pagamentos.infraestrutura.mercadopago import FakeMercadoPagoOrdersProvider
from src.shared.banco import Base
from src.worker import consumir_comandos_pull, publicar_lote_outbox


def _criar_sessao(tmp_path):
    db_file = tmp_path / "billing_p4.sqlite"
    engine = create_engine(f"sqlite:///{db_file}")
    Base.metadata.create_all(bind=engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


def test_billing_consumidor_fluxo_completo_deduplicacao_e_compensacao(tmp_path):
    SessionLocal = _criar_sessao(tmp_path)
    provedor = FakeMercadoPagoOrdersProvider(webhook_secret="sec-p4")
    saga_id = uuid4()
    os_id = uuid4()
    corr_id = uuid4()
    cli_id = uuid4()

    cmd_gerar = {
        "event_id": str(uuid4()),
        "event_type": "GerarOrcamento.v1",
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "producer": "oficina-os",
        "saga_id": str(saga_id),
        "os_id": str(os_id),
        "correlation_id": str(corr_id),
        "causation_id": str(saga_id),
        "aggregate_version": 2,
        "payload": {
            "rodada": 1,
            "versao": 1,
            "cliente_id": str(cli_id),
            "itens_servico": [
                {
                    "servico_id": str(uuid4()),
                    "descricao": "Alinhamento",
                    "preco_unitario_centavos": 12000,
                }
            ],
            "itens_peca": [
                {
                    "peca_id": str(uuid4()),
                    "descricao": "Pastilha",
                    "quantidade": 2,
                    "preco_unitario_centavos": 9000,
                }
            ],
        },
    }

    with SessionLocal() as db:
        cons = BillingConsumidor(db, provedor, environment="homolog")
        assert cons.processar_comando(cmd_gerar) == "PROCESSADO"
        # Reentrega na janela commit/ACK -> DUPLICADO sem criar segundo orçamento
        assert cons.processar_comando(cmd_gerar) == "DUPLICADO"
        assert db.query(OrcamentoModel).count() == 1
        orc_id = db.query(OrcamentoModel).one().id

        cmd_cob = {
            "event_id": str(uuid4()),
            "event_type": "CriarCobranca.v1",
            "schema_version": 1,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "producer": "oficina-os",
            "saga_id": str(saga_id),
            "os_id": str(os_id),
            "correlation_id": str(corr_id),
            "causation_id": str(saga_id),
            "aggregate_version": 4,
            "payload": {
                "rodada": 1,
                "orcamento_id": orc_id,
                "orcamento_versao": 1,
                "cliente_id": str(cli_id),
                "total_centavos": 30000,
                "moeda": "BRL",
            },
        }
        assert cons.processar_comando(cmd_cob) == "PROCESSADO"
        assert cons.processar_comando(cmd_cob) == "DUPLICADO"
        assert db.query(PagamentoModel).count() == 1
        pag = db.query(PagamentoModel).one()
        assert pag.total_centavos == 30000
        assert pag.provider_order_id is not None

        # Simula aprovação financeira no provedor fake e executa estorno com barreira
        provedor.simular_pagamento_aprovado(pag.provider_order_id)
        barreira_id = str(uuid4())
        comp_id = str(uuid4())
        cmd_estorno = {
            "event_id": str(uuid4()),
            "event_type": "EstornarPagamento.v1",
            "schema_version": 1,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "producer": "oficina-os",
            "saga_id": str(saga_id),
            "os_id": str(os_id),
            "correlation_id": str(corr_id),
            "causation_id": barreira_id,
            "aggregate_version": 7,
            "payload": {
                "rodada": 1,
                "compensacao_id": comp_id,
                "motivo": "FALHA_ENFILEIRAMENTO",
                "pagamento_id": pag.id,
                "total_centavos": 30000,
                "moeda": "BRL",
                "execucao_cancelada_event_id": barreira_id,
            },
        }
        assert cons.processar_comando(cmd_estorno) == "PROCESSADO"
        db.refresh(pag)
        assert pag.status == "ESTORNADO"
        assert (
            db.query(OutboxEventoModel)
            .filter_by(tipo_evento="PagamentoEstornado.v1")
            .count()
            == 1
        )


def test_billing_worker_janela_publish_e_ack(tmp_path):
    SessionLocal = _criar_sessao(tmp_path)
    provedor = FakeMercadoPagoOrdersProvider(webhook_secret="sec-p4")

    cmd_gerar = {
        "event_id": str(uuid4()),
        "event_type": "GerarOrcamento.v1",
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "producer": "oficina-os",
        "saga_id": str(uuid4()),
        "os_id": str(uuid4()),
        "correlation_id": str(uuid4()),
        "causation_id": str(uuid4()),
        "aggregate_version": 2,
        "payload": {
            "rodada": 1,
            "versao": 1,
            "cliente_id": str(uuid4()),
            "itens_servico": [
                {
                    "servico_id": str(uuid4()),
                    "descricao": "Revisão",
                    "preco_unitario_centavos": 15000,
                }
            ],
            "itens_peca": [],
        },
    }
    msg = SimpleNamespace(
        ack_id="ack-b1",
        message=SimpleNamespace(
            data=json.dumps(cmd_gerar).encode("utf-8"),
            attributes={"traceparent": "00-123-456-01"},
        ),
    )
    sub = MagicMock()
    sub.subscription_path.return_value = "projects/p/subscriptions/sub-b"
    sub.pull.return_value = SimpleNamespace(received_messages=[msg])

    assert (
        consumir_comandos_pull(
            SessionLocal, sub, provedor, project_id="p", subscription_name="sub-b"
        )
        == 1
    )
    assert sub.acknowledge.call_count == 1

    with SessionLocal() as db:
        pub_err = MagicMock()
        pub_err.topic_path.return_value = "projects/p/topics/t"
        fut_err = MagicMock()
        fut_err.result.side_effect = RuntimeError("PubSub offline")
        pub_err.publish.return_value = fut_err
        assert publicar_lote_outbox(db, pub_err, project_id="p") == 0

        ev = db.query(OutboxEventoModel).one()
        assert ev.status == "FALHA_AGENDADA"
        ev.next_attempt_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        db.commit()

        pub_ok = MagicMock()
        pub_ok.topic_path.return_value = "projects/p/topics/t"
        fut_ok = MagicMock()
        fut_ok.result.return_value = "msg-b1"
        pub_ok.publish.return_value = fut_ok
        assert publicar_lote_outbox(db, pub_ok, project_id="p") == 1
        db.refresh(ev)
        assert ev.status == "PROCESSADO"


def test_billing_webhook_confirma_pagamento_e_emite_outbox(tmp_path):
    db_file = tmp_path / "billing_wh.sqlite"
    cfg = Configuracoes(
        database_url=f"sqlite:///{db_file}",
        mp_webhook_secret="sec-wh",
        mp_provider_mode="fake",
        dd_trace_enabled=False,
    )
    app = criar_app(cfg)
    cli = TestClient(app)

    provedor: FakeMercadoPagoOrdersProvider = app.state.provedor_pagamento
    pag_id = str(uuid4())
    ordem = provedor.criar_cobranca(
        pagamento_id=pag_id,
        total_centavos=20000,
        moeda="BRL",
        descricao="OS 1",
        idempotency_key=f"k-{pag_id}",
    )

    with app.state.session_factory() as db:
        db.add(
            PagamentoModel(
                id=pag_id,
                orcamento_id=str(uuid4()),
                os_id=str(uuid4()),
                saga_id=str(uuid4()),
                cliente_id=str(uuid4()),
                rodada=1,
                provider_order_id=ordem.provider_order_id,
                qr_data=ordem.qr_data,
                status="PENDENTE",
                total_centavos=20000,
                moeda="BRL",
            )
        )
        db.commit()

    headers, body = provedor.simular_pagamento_aprovado(ordem.provider_order_id)
    resp = cli.post("/webhooks/mercadopago", json=body, headers=headers)
    assert resp.status_code == 200

    with app.state.session_factory() as db:
        assert (
            db.query(OutboxEventoModel)
            .filter_by(tipo_evento="PagamentoConfirmado.v1")
            .count()
            == 1
        )
