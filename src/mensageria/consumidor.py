from datetime import datetime, timezone
import hashlib
import json
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.mensageria.contratos import validar_comando_billing_v1
from src.mensageria.outbox_inbox import InboxEventoModel, OutboxEventoModel
from src.orcamentos.dominio.entidades import (
    ItemOrcamento,
    TipoItemOrcamento,
    criar_orcamento_congelado,
)
from src.orcamentos.infraestrutura.modelos import (
    OrcamentoModel,
    PagamentoModel,
    ProviderOperationModel,
)
from src.orcamentos.infraestrutura.repositorios import OrcamentoRepositorioSql
from src.pagamentos.infraestrutura.mercadopago import (
    ErroComunicacaoProvedorError,
    ErroOperacaoProvedorError,
    ProvedorPagamento,
    StatusPagamentoProvedor,
)


class BillingConsumidor:
    """
    Consumidor transacional de comandos da Saga para o serviço de Billing.
    Garante deduplicação por (consumer, event_id), persistência do efeito e
    enfileiramento do evento de resposta na Outbox dentro da mesma transação SQL.
    """

    def __init__(
        self,
        db: Session,
        provedor: ProvedorPagamento,
        *,
        environment: str = "homolog",
    ) -> None:
        self.db = db
        self.provedor = provedor
        self.environment = environment
        self.repo = OrcamentoRepositorioSql(db)

    def _publicar_outbox(
        self,
        *,
        event_type: str,
        saga_id: UUID,
        os_id: UUID,
        correlation_id: UUID,
        causation_id: UUID,
        aggregate_version: int,
        payload: dict[str, Any],
        traceparent: str | None = None,
    ) -> UUID:
        event_id = uuid4()
        envelope = {
            "event_id": str(event_id),
            "event_type": event_type,
            "schema_version": 1,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "producer": "oficina-billing",
            "saga_id": str(saga_id),
            "os_id": str(os_id),
            "correlation_id": str(correlation_id),
            "causation_id": str(causation_id),
            "aggregate_version": aggregate_version,
            "payload": payload,
        }
        self.db.add(
            OutboxEventoModel(
                id=str(event_id),
                event_id=str(event_id),
                topic=f"oficina-{self.environment}-eventos-v1",
                tipo_evento=event_type,
                payload=envelope,
                traceparent=traceparent,
                status="PENDENTE",
            )
        )
        return event_id

    def processar_comando(
        self,
        dados: dict[str, Any],
        *,
        consumer: str = "billing-comandos",
        traceparent: str | None = None,
    ) -> str:
        envelope = validar_comando_billing_v1(dados)

        ja_processado = (
            self.db.query(InboxEventoModel)
            .filter(
                InboxEventoModel.consumer == consumer,
                InboxEventoModel.event_id == str(envelope.event_id),
            )
            .first()
        )
        if ja_processado is not None:
            return "DUPLICADO"

        try:
            self.db.add(
                InboxEventoModel(
                    consumer=consumer,
                    event_id=str(envelope.event_id),
                    event_type=envelope.event_type,
                    saga_id=str(envelope.saga_id),
                )
            )
            self.db.flush()
        except IntegrityError:
            self.db.rollback()
            return "DUPLICADO"

        p = envelope.payload
        evt = envelope.event_type

        if evt == "GerarOrcamento.v1":
            if self.repo.buscar_tombstone_por_saga(envelope.saga_id) is not None:
                self.db.commit()
                return "BLOQUEADO_TOMBSTONE"

            itens: list[ItemOrcamento] = []
            for s in p["itens_servico"]:
                itens.append(
                    ItemOrcamento(
                        id=uuid4(),
                        tipo=TipoItemOrcamento.SERVICO,
                        referencia_id=UUID(str(s["servico_id"])),
                        descricao=str(s["descricao"]),
                        quantidade=1,
                        preco_unitario_centavos=int(s["preco_unitario_centavos"]),
                    )
                )
            for pc in p["itens_peca"]:
                itens.append(
                    ItemOrcamento(
                        id=uuid4(),
                        tipo=TipoItemOrcamento.PECA,
                        referencia_id=UUID(str(pc["peca_id"])),
                        descricao=str(pc["descricao"]),
                        quantidade=int(pc["quantidade"]),
                        preco_unitario_centavos=int(pc["preco_unitario_centavos"]),
                    )
                )

            orcamento = criar_orcamento_congelado(
                os_id=envelope.os_id,
                saga_id=envelope.saga_id,
                cliente_id=UUID(str(p["cliente_id"])),
                rodada=int(p["rodada"]),
                versao=int(p["versao"]),
                itens=itens,
            )
            self.repo.salvar(orcamento)
            self._publicar_outbox(
                event_type="OrcamentoGerado.v1",
                saga_id=envelope.saga_id,
                os_id=envelope.os_id,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.event_id,
                aggregate_version=1,
                payload={
                    "rodada": orcamento.rodada,
                    "orcamento_id": str(orcamento.id),
                    "versao": orcamento.versao,
                    "total_centavos": orcamento.total_centavos,
                    "moeda": orcamento.moeda,
                },
                traceparent=traceparent,
            )

        elif evt == "CriarCobranca.v1":
            if self.repo.buscar_tombstone_por_saga(envelope.saga_id) is not None:
                self.db.commit()
                return "BLOQUEADO_TOMBSTONE"

            pag_existente = (
                self.db.query(PagamentoModel)
                .filter(PagamentoModel.saga_id == str(envelope.saga_id))
                .first()
            )
            if pag_existente is None:
                pag_id = uuid4()
                idem_key = f"create-{envelope.saga_id}-{p['rodada']}"
                req_hash = hashlib.sha256(
                    json.dumps(p, sort_keys=True).encode("utf-8")
                ).hexdigest()

                op = ProviderOperationModel(
                    pagamento_id=str(pag_id),
                    saga_id=str(envelope.saga_id),
                    idempotency_key=idem_key,
                    tipo="CREATE_ORDER",
                    estado="PENDENTE",
                    request_hash=req_hash,
                )
                self.db.add(op)
                self.db.flush()

                try:
                    ordem = self.provedor.criar_cobranca(
                        pagamento_id=str(pag_id),
                        total_centavos=int(p["total_centavos"]),
                        moeda=str(p["moeda"]),
                        descricao=f"OS {envelope.os_id}",
                        idempotency_key=idem_key,
                    )
                except ErroComunicacaoProvedorError:
                    op.estado = "DESCONHECIDO"
                    self.db.add(
                        PagamentoModel(
                            id=str(pag_id),
                            orcamento_id=str(p["orcamento_id"]),
                            os_id=str(envelope.os_id),
                            saga_id=str(envelope.saga_id),
                            cliente_id=str(p["cliente_id"]),
                            rodada=int(p["rodada"]),
                            provider_order_id=None,
                            qr_data=None,
                            status="PENDENTE_RECONCILIACAO",
                            total_centavos=int(p["total_centavos"]),
                            moeda=str(p["moeda"]),
                        )
                    )
                    self.db.commit()
                    return "PENDENTE_RECONCILIACAO"

                op.estado = "SUCESSO"
                op.provider_id = ordem.provider_order_id

                pag_existente = PagamentoModel(
                    id=str(pag_id),
                    orcamento_id=str(p["orcamento_id"]),
                    os_id=str(envelope.os_id),
                    saga_id=str(envelope.saga_id),
                    cliente_id=str(p["cliente_id"]),
                    rodada=int(p["rodada"]),
                    provider_order_id=ordem.provider_order_id,
                    qr_data=ordem.qr_data,
                    status=ordem.status.value,
                    total_centavos=ordem.total_centavos,
                    moeda=ordem.moeda,
                )
                self.db.add(pag_existente)

            if pag_existente.status == "PENDENTE_RECONCILIACAO":
                self.db.commit()
                return "PENDENTE_RECONCILIACAO"

            self._publicar_outbox(
                event_type="CobrancaCriada.v1",
                saga_id=envelope.saga_id,
                os_id=envelope.os_id,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.event_id,
                aggregate_version=2,
                payload={
                    "rodada": int(p["rodada"]),
                    "orcamento_id": str(p["orcamento_id"]),
                    "pagamento_id": pag_existente.id,
                    "provider_order_id": str(pag_existente.provider_order_id),
                    "qr_data": str(pag_existente.qr_data or ""),
                    "total_centavos": int(pag_existente.total_centavos),
                    "moeda": pag_existente.moeda,
                },
                traceparent=traceparent,
            )

        elif evt == "CancelarOrcamento.v1":
            barreira_id = UUID(str(p["execucao_cancelada_event_id"]))
            self.repo.registrar_tombstone_barreira(
                saga_id=envelope.saga_id,
                os_id=envelope.os_id,
                rodada=int(p["rodada"]),
                execucao_cancelada_event_id=barreira_id,
                motivo=str(p["motivo"]),
            )
            orc = (
                self.db.query(OrcamentoModel)
                .filter(OrcamentoModel.id == str(p["orcamento_id"]))
                .first()
            )
            resultado = "NAO_EXISTIA"
            if orc is not None:
                orc.status = "CANCELADO"
                resultado = "CANCELADO"
            self._publicar_outbox(
                event_type="OrcamentoCancelado.v1",
                saga_id=envelope.saga_id,
                os_id=envelope.os_id,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.event_id,
                aggregate_version=3,
                payload={
                    "rodada": int(p["rodada"]),
                    "compensacao_id": str(p["compensacao_id"]),
                    "orcamento_id": str(p["orcamento_id"]),
                    "resultado": resultado,
                    "motivo": str(p["motivo"]),
                },
                traceparent=traceparent,
            )

        elif evt == "EstornarPagamento.v1":
            barreira_id = UUID(str(p["execucao_cancelada_event_id"]))
            self.repo.registrar_tombstone_barreira(
                saga_id=envelope.saga_id,
                os_id=envelope.os_id,
                rodada=int(p["rodada"]),
                execucao_cancelada_event_id=barreira_id,
                motivo=str(p["motivo"]),
            )
            pag = self.repo.buscar_pagamento_por_id(UUID(str(p["pagamento_id"])))
            if pag is None:
                self.db.commit()
                return "PROCESSADO"

            if pag.status == "PENDENTE" and pag.provider_order_id:
                try:
                    ordem_atual = self.provedor.consultar(pag.provider_order_id)
                    if ordem_atual.status == StatusPagamentoProvedor.CONFIRMADO:
                        pag.status = StatusPagamentoProvedor.CONFIRMADO.value
                except (ErroComunicacaoProvedorError, ErroOperacaoProvedorError):
                    pass

            if pag.status in {
                "PENDENTE",
                "RECUSADO",
                "CANCELADO",
                "PENDENTE_RECONCILIACAO",
            }:
                if pag.provider_order_id and pag.status == "PENDENTE":
                    try:
                        self.provedor.cancelar(
                            pag.provider_order_id,
                            idempotency_key=f"cancel-{pag.id}",
                        )
                    except (ErroComunicacaoProvedorError, ErroOperacaoProvedorError):
                        pass
                pag.status = "CANCELADO"
                self._publicar_outbox(
                    event_type="PagamentoEstornado.v1",
                    saga_id=envelope.saga_id,
                    os_id=envelope.os_id,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.event_id,
                    aggregate_version=4,
                    payload={
                        "rodada": int(p["rodada"]),
                        "compensacao_id": str(p["compensacao_id"]),
                        "pagamento_id": pag.id,
                        "provider_order_id": pag.provider_order_id or "",
                        "total_centavos": pag.total_centavos,
                        "moeda": pag.moeda,
                        "resultado": "CANCELADO_SEM_COBRANCA",
                        "motivo": str(p["motivo"]),
                    },
                    traceparent=traceparent,
                )
            elif pag.provider_order_id:
                idem_refund = f"refund-{pag.id}"
                op = (
                    self.db.query(ProviderOperationModel)
                    .filter(ProviderOperationModel.idempotency_key == idem_refund)
                    .first()
                )
                if op is None:
                    op = ProviderOperationModel(
                        pagamento_id=pag.id,
                        saga_id=str(envelope.saga_id),
                        idempotency_key=idem_refund,
                        tipo="REFUND",
                        estado="PENDENTE",
                        provider_id=pag.provider_order_id,
                        request_hash=hashlib.sha256(
                            json.dumps(p, sort_keys=True).encode("utf-8")
                        ).hexdigest(),
                    )
                    self.db.add(op)
                    self.db.flush()

                try:
                    estorno = self.provedor.estornar(
                        pag.provider_order_id,
                        idempotency_key=idem_refund,
                    )
                    if estorno.status == StatusPagamentoProvedor.ESTORNADO:
                        op.estado = "SUCESSO"
                        pag.status = "ESTORNADO"
                        self._publicar_outbox(
                            event_type="PagamentoEstornado.v1",
                            saga_id=envelope.saga_id,
                            os_id=envelope.os_id,
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.event_id,
                            aggregate_version=4,
                            payload={
                                "rodada": int(p["rodada"]),
                                "compensacao_id": str(p["compensacao_id"]),
                                "pagamento_id": pag.id,
                                "provider_order_id": pag.provider_order_id,
                                "total_centavos": pag.total_centavos,
                                "moeda": pag.moeda,
                                "resultado": "ESTORNADO",
                                "motivo": str(p["motivo"]),
                            },
                            traceparent=traceparent,
                        )
                except (ErroComunicacaoProvedorError, ErroOperacaoProvedorError) as exc:
                    op.estado = (
                        "DESCONHECIDO"
                        if isinstance(exc, ErroComunicacaoProvedorError)
                        else "FALHA"
                    )
                    pag.status = "FALHA_ESTORNO"
                    self._publicar_outbox(
                        event_type="PagamentoEstornoFalhou.v1",
                        saga_id=envelope.saga_id,
                        os_id=envelope.os_id,
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.event_id,
                        aggregate_version=4,
                        payload={
                            "rodada": int(p["rodada"]),
                            "compensacao_id": str(p["compensacao_id"]),
                            "pagamento_id": pag.id,
                            "provider_order_id": pag.provider_order_id,
                            "resultado": "FALHA_DEFINITIVA",
                            "motivo": str(exc),
                        },
                        traceparent=traceparent,
                    )

        self.db.commit()
        return "PROCESSADO"

    def reconciliar_operacoes_pendentes(self) -> int:
        """
        Reconcilia operações em estado DESCONHECIDO (timeout de rede/5xx) ou pagamentos
        em PENDENTE_RECONCILIACAO consultando o provedor sem duplicar cobranças ou estornos.
        """
        reconciliadas = 0
        ops = (
            self.db.query(ProviderOperationModel)
            .filter(ProviderOperationModel.estado == "DESCONHECIDO")
            .all()
        )
        for op in ops:
            pag = (
                self.db.query(PagamentoModel)
                .filter(PagamentoModel.id == op.pagamento_id)
                .first()
            )
            if pag is None:
                continue

            if op.tipo == "CREATE_ORDER":
                ordem = (
                    self.provedor.consultar(op.provider_id)
                    if op.provider_id
                    else self.provedor.buscar_por_external_reference(pag.id)
                )
                if ordem is None:
                    ordem = self.provedor.criar_cobranca(
                        pagamento_id=pag.id,
                        total_centavos=pag.total_centavos,
                        moeda=pag.moeda,
                        descricao=f"OS {pag.os_id}",
                        idempotency_key=op.idempotency_key,
                    )
                op.estado = "SUCESSO"
                op.provider_id = ordem.provider_order_id
                pag.provider_order_id = ordem.provider_order_id
                pag.qr_data = ordem.qr_data
                pag.status = ordem.status.value
                self._publicar_outbox(
                    event_type="CobrancaCriada.v1",
                    saga_id=UUID(pag.saga_id),
                    os_id=UUID(pag.os_id),
                    correlation_id=UUID(pag.saga_id),
                    causation_id=UUID(pag.id),
                    aggregate_version=2,
                    payload={
                        "rodada": pag.rodada,
                        "orcamento_id": pag.orcamento_id,
                        "pagamento_id": pag.id,
                        "provider_order_id": str(pag.provider_order_id),
                        "qr_data": str(pag.qr_data or ""),
                        "total_centavos": int(pag.total_centavos),
                        "moeda": pag.moeda,
                    },
                )
                reconciliadas += 1

            elif op.tipo == "REFUND" and pag.provider_order_id:
                try:
                    ordem = self.provedor.consultar(pag.provider_order_id)
                    if ordem.status != StatusPagamentoProvedor.ESTORNADO:
                        ordem = self.provedor.estornar(
                            pag.provider_order_id, idempotency_key=op.idempotency_key
                        )
                except (ErroComunicacaoProvedorError, ErroOperacaoProvedorError):
                    continue
                if ordem.status == StatusPagamentoProvedor.ESTORNADO:
                    op.estado = "SUCESSO"
                    pag.status = "ESTORNADO"
                    self._publicar_outbox(
                        event_type="PagamentoEstornado.v1",
                        saga_id=UUID(pag.saga_id),
                        os_id=UUID(pag.os_id),
                        correlation_id=UUID(pag.saga_id),
                        causation_id=UUID(pag.id),
                        aggregate_version=4,
                        payload={
                            "rodada": pag.rodada,
                            "compensacao_id": pag.saga_id,
                            "pagamento_id": pag.id,
                            "provider_order_id": pag.provider_order_id,
                            "total_centavos": pag.total_centavos,
                            "moeda": pag.moeda,
                            "resultado": "ESTORNADO",
                            "motivo": "RECONCILIACAO_ESTORNO",
                        },
                    )
                    reconciliadas += 1

        pags_rec = (
            self.db.query(PagamentoModel)
            .filter(PagamentoModel.status == "PENDENTE_RECONCILIACAO")
            .all()
        )
        for pag in pags_rec:
            if not pag.provider_order_id:
                continue
            try:
                ordem = self.provedor.consultar(pag.provider_order_id)
            except ErroComunicacaoProvedorError:
                continue
            if ordem.status == StatusPagamentoProvedor.CONFIRMADO:
                pag.status = StatusPagamentoProvedor.CONFIRMADO.value
                self._publicar_outbox(
                    event_type="PagamentoConfirmado.v1",
                    saga_id=UUID(pag.saga_id),
                    os_id=UUID(pag.os_id),
                    correlation_id=UUID(pag.saga_id),
                    causation_id=UUID(pag.id),
                    aggregate_version=4,
                    payload={
                        "orcamento_id": pag.orcamento_id,
                        "pagamento_id": pag.id,
                        "provider_order_id": pag.provider_order_id,
                        "total_centavos": pag.total_centavos,
                        "moeda": pag.moeda,
                        "orcamento_versao": 1,
                        "itens_servico": [],
                        "itens_peca": [],
                    },
                )
                reconciliadas += 1
            elif ordem.status in {
                StatusPagamentoProvedor.RECUSADO,
                StatusPagamentoProvedor.CANCELADO,
            }:
                pag.status = ordem.status.value
                self._publicar_outbox(
                    event_type="PagamentoRecusado.v1",
                    saga_id=UUID(pag.saga_id),
                    os_id=UUID(pag.os_id),
                    correlation_id=UUID(pag.saga_id),
                    causation_id=UUID(pag.id),
                    aggregate_version=4,
                    payload={
                        "rodada": pag.rodada,
                        "orcamento_id": pag.orcamento_id,
                        "pagamento_id": pag.id,
                        "provider_order_id": pag.provider_order_id,
                        "motivo": ordem.raw_status,
                    },
                )
                reconciliadas += 1

        if reconciliadas > 0:
            self.db.commit()
        return reconciliadas
