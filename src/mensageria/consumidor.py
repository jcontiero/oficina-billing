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
    ResultadoOrdemProvedor,
    StatusPagamentoProvedor,
    validar_confirmacao_cobranca,
)

EVT_PAGAMENTO_ESTORNADO = "PagamentoEstornado.v1"
EVT_PAGAMENTO_ESTORNO_PENDENTE = "PagamentoEstornoPendente.v1"
EVT_PAGAMENTO_ESTORNO_FALHOU = "PagamentoEstornoFalhou.v1"
EVT_COBRANCA_CANCELADA = "CobrancaCancelada.v1"


def confirmar_pagamento_aprovado(
    db: Session,
    pag: PagamentoModel,
    ordem: ResultadoOrdemProvedor,
    *,
    environment: str,
    traceparent: str | None = None,
) -> bool:
    """
    Caminho único de confirmação financeira (usado por webhook e reconciliação):
    valida valor/moeda/referência externa e emite PagamentoConfirmado.v1 com versão e
    itens reais do orçamento aprovado.
    """
    if pag.status == StatusPagamentoProvedor.CONFIRMADO.value:
        return False

    validar_confirmacao_cobranca(
        ordem,
        pagamento_id_esperado=pag.id,
        total_centavos_esperado=pag.total_centavos,
        moeda_esperada=pag.moeda,
    )
    pag.status = StatusPagamentoProvedor.CONFIRMADO.value

    orc = OrcamentoRepositorioSql(db).buscar_por_id(UUID(pag.orcamento_id))
    itens_servico: list[dict[str, Any]] = []
    itens_peca: list[dict[str, Any]] = []
    if orc is not None:
        itens_servico = [
            {
                "servico_id": str(i.referencia_id),
                "descricao": i.descricao,
                "preco_unitario_centavos": i.preco_unitario_centavos,
            }
            for i in orc.itens
            if i.tipo == TipoItemOrcamento.SERVICO
        ]
        itens_peca = [
            {
                "peca_id": str(i.referencia_id),
                "descricao": i.descricao,
                "quantidade": i.quantidade,
                "preco_unitario_centavos": i.preco_unitario_centavos,
            }
            for i in orc.itens
            if i.tipo == TipoItemOrcamento.PECA
        ]

    evt_id = uuid4()
    db.add(
        OutboxEventoModel(
            id=str(evt_id),
            event_id=str(evt_id),
            topic=f"oficina-{environment}-eventos-v1",
            tipo_evento="PagamentoConfirmado.v1",
            payload={
                "event_id": str(evt_id),
                "event_type": "PagamentoConfirmado.v1",
                "schema_version": 1,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "producer": "oficina-billing",
                "saga_id": pag.saga_id,
                "os_id": pag.os_id,
                "correlation_id": pag.saga_id,
                "causation_id": str(evt_id),
                "aggregate_version": 4,
                "payload": {
                    "orcamento_id": pag.orcamento_id,
                    "pagamento_id": pag.id,
                    "provider_order_id": ordem.provider_order_id,
                    "total_centavos": pag.total_centavos,
                    "moeda": pag.moeda,
                    "orcamento_versao": orc.versao if orc else 1,
                    "itens_servico": itens_servico,
                    "itens_peca": itens_peca,
                },
            },
            traceparent=traceparent,
            status="PENDENTE",
        )
    )
    return True


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

    def _ja_processado_inbox(self, consumer: str, envelope: Any) -> bool:
        return (
            self.db.query(InboxEventoModel)
            .filter(
                InboxEventoModel.consumer == consumer,
                InboxEventoModel.event_id == str(envelope.event_id),
            )
            .first()
            is not None
        )

    def _registrar_inbox(self, consumer: str, envelope: Any) -> bool:
        if self._ja_processado_inbox(consumer, envelope):
            return False

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
            return False
        return True

    def _processar_gerar_orcamento(self, envelope: Any, traceparent: str | None) -> str:
        if self.repo.buscar_tombstone_por_saga(envelope.saga_id) is not None:
            self.db.commit()
            return "BLOQUEADO_TOMBSTONE"

        p = envelope.payload
        itens = [
            ItemOrcamento(
                id=uuid4(),
                tipo=TipoItemOrcamento.SERVICO,
                referencia_id=UUID(str(s["servico_id"])),
                descricao=str(s["descricao"]),
                quantidade=1,
                preco_unitario_centavos=int(s["preco_unitario_centavos"]),
            )
            for s in p["itens_servico"]
        ] + [
            ItemOrcamento(
                id=uuid4(),
                tipo=TipoItemOrcamento.PECA,
                referencia_id=UUID(str(pc["peca_id"])),
                descricao=str(pc["descricao"]),
                quantidade=int(pc["quantidade"]),
                preco_unitario_centavos=int(pc["preco_unitario_centavos"]),
            )
            for pc in p["itens_peca"]
        ]

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
        self.db.commit()
        return "PROCESSADO"

    def _processar_criar_cobranca(
        self,
        envelope: Any,
        traceparent: str | None,
        *,
        consumer: str = "billing-comandos",
    ) -> str:
        if self.repo.buscar_tombstone_por_saga(envelope.saga_id) is not None:
            self._registrar_inbox(consumer, envelope)
            self.db.commit()
            return "BLOQUEADO_TOMBSTONE"

        p = envelope.payload
        pag_existente = (
            self.db.query(PagamentoModel)
            .filter(PagamentoModel.saga_id == str(envelope.saga_id))
            .first()
        )
        idem_key = f"create-{envelope.saga_id}-{p['rodada']}"
        if pag_existente is None:
            pag_id = str(uuid4())
            op = self._obter_ou_criar_operacao(
                pagamento_id=pag_id,
                saga_id=envelope.saga_id,
                idempotency_key=idem_key,
                tipo="CREATE_ORDER",
                provider_id=None,
                payload=p,
            )
            pag_existente = PagamentoModel(
                id=pag_id,
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
            self.db.add(pag_existente)
            # Persiste a intenção durável antes de qualquer chamada externa
            self.db.commit()
        else:
            op = self._obter_ou_criar_operacao(
                pagamento_id=pag_existente.id,
                saga_id=envelope.saga_id,
                idempotency_key=idem_key,
                tipo="CREATE_ORDER",
                provider_id=pag_existente.provider_order_id,
                payload=p,
            )

        if pag_existente.status == "PENDENTE_RECONCILIACAO":
            try:
                ordem = (
                    self.provedor.consultar(op.provider_id)
                    if op.provider_id
                    else self.provedor.buscar_por_external_reference(pag_existente.id)
                )
                if ordem is None:
                    ordem = self.provedor.criar_cobranca(
                        pagamento_id=pag_existente.id,
                        total_centavos=int(p["total_centavos"]),
                        moeda=str(p["moeda"]),
                        descricao=f"OS {envelope.os_id}",
                        idempotency_key=op.idempotency_key,
                    )
            except ErroComunicacaoProvedorError:
                op.estado = "DESCONHECIDO"
                self._registrar_inbox(consumer, envelope)
                self.db.commit()
                return "PENDENTE_RECONCILIACAO"

            op.estado = "SUCESSO"
            op.provider_id = ordem.provider_order_id
            pag_existente.provider_order_id = ordem.provider_order_id
            pag_existente.qr_data = ordem.qr_data
            pag_existente.status = ordem.status.value

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
        self._registrar_inbox(consumer, envelope)
        self.db.commit()
        return "PROCESSADO"

    def _processar_cancelar_orcamento(
        self, envelope: Any, traceparent: str | None
    ) -> str:
        p = envelope.payload
        self.repo.registrar_tombstone_barreira(
            saga_id=envelope.saga_id,
            os_id=envelope.os_id,
            rodada=int(p["rodada"]),
            execucao_cancelada_event_id=UUID(str(p["execucao_cancelada_event_id"])),
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
        self.db.commit()
        return "PROCESSADO"

    def _obter_ou_criar_operacao(
        self,
        *,
        pagamento_id: str,
        saga_id: UUID,
        idempotency_key: str,
        tipo: str,
        provider_id: str | None,
        payload: dict[str, Any],
    ) -> ProviderOperationModel:
        op = (
            self.db.query(ProviderOperationModel)
            .filter(ProviderOperationModel.idempotency_key == idempotency_key)
            .first()
        )
        if op is None:
            op = ProviderOperationModel(
                pagamento_id=pagamento_id,
                saga_id=str(saga_id),
                idempotency_key=idempotency_key,
                tipo=tipo,
                estado="PENDENTE",
                provider_id=provider_id,
                request_hash=hashlib.sha256(
                    json.dumps(payload, sort_keys=True).encode("utf-8")
                ).hexdigest(),
            )
            self.db.add(op)
            self.db.flush()
        return op

    def _executar_estorno_pago(
        self,
        pag: PagamentoModel,
        envelope: Any,
        traceparent: str | None,
        *,
        evento_sucesso: str = EVT_PAGAMENTO_ESTORNADO,
    ) -> None:
        p = envelope.payload
        idem_refund = f"refund-{pag.id}"
        op = self._obter_ou_criar_operacao(
            pagamento_id=pag.id,
            saga_id=envelope.saga_id,
            idempotency_key=idem_refund,
            tipo="REFUND",
            provider_id=pag.provider_order_id,
            payload=p,
        )
        self.db.commit()
        try:
            estorno = self.provedor.estornar(
                str(pag.provider_order_id),
                idempotency_key=idem_refund,
            )
            if estorno.status == StatusPagamentoProvedor.ESTORNADO:
                op.estado = "SUCESSO"
                pag.status = "ESTORNADO"
                self._publicar_outbox(
                    event_type=evento_sucesso,
                    saga_id=envelope.saga_id,
                    os_id=envelope.os_id,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.event_id,
                    aggregate_version=4,
                    payload={
                        "rodada": int(p["rodada"]),
                        "compensacao_id": str(p["compensacao_id"]),
                        "pagamento_id": pag.id,
                        "provider_order_id": str(pag.provider_order_id),
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
                event_type=EVT_PAGAMENTO_ESTORNO_FALHOU,
                saga_id=envelope.saga_id,
                os_id=envelope.os_id,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.event_id,
                aggregate_version=4,
                payload={
                    "rodada": int(p["rodada"]),
                    "compensacao_id": str(p["compensacao_id"]),
                    "pagamento_id": pag.id,
                    "provider_order_id": str(pag.provider_order_id or ""),
                    "resultado": "FALHA_DEFINITIVA",
                    "motivo": str(exc),
                },
                traceparent=traceparent,
            )

    def _executar_cancelamento_nao_pago(
        self,
        pag: PagamentoModel,
        envelope: Any,
        traceparent: str | None,
        *,
        evento_sucesso: str = EVT_PAGAMENTO_ESTORNADO,
    ) -> None:
        p = envelope.payload
        if not pag.provider_order_id:
            try:
                ordem_remota = self.provedor.buscar_por_external_reference(pag.id)
            except ErroComunicacaoProvedorError as exc:
                pag.status = "CANCELAMENTO_PENDENTE"
                self._publicar_outbox(
                    event_type=EVT_PAGAMENTO_ESTORNO_PENDENTE,
                    saga_id=envelope.saga_id,
                    os_id=envelope.os_id,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.event_id,
                    aggregate_version=4,
                    payload={
                        "rodada": int(p["rodada"]),
                        "compensacao_id": str(p["compensacao_id"]),
                        "pagamento_id": pag.id,
                        "resultado": "PENDENTE_RECONCILIACAO",
                        "motivo": str(exc),
                    },
                    traceparent=traceparent,
                )
                return
            if ordem_remota is not None:
                pag.provider_order_id = ordem_remota.provider_order_id
            else:
                pag.status = "CANCELADO"
                self._publicar_outbox(
                    event_type=evento_sucesso,
                    saga_id=envelope.saga_id,
                    os_id=envelope.os_id,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.event_id,
                    aggregate_version=4,
                    payload={
                        "rodada": int(p["rodada"]),
                        "compensacao_id": str(p["compensacao_id"]),
                        "pagamento_id": pag.id,
                        "provider_order_id": "",
                        "total_centavos": pag.total_centavos,
                        "moeda": pag.moeda,
                        "resultado": "CANCELADO_SEM_COBRANCA",
                        "motivo": str(p["motivo"]),
                    },
                    traceparent=traceparent,
                )
                return

        idem_cancel = f"cancel-{pag.id}"
        op = self._obter_ou_criar_operacao(
            pagamento_id=pag.id,
            saga_id=envelope.saga_id,
            idempotency_key=idem_cancel,
            tipo="CANCEL_ORDER",
            provider_id=pag.provider_order_id,
            payload=p,
        )
        try:
            ordem_atual = self.provedor.consultar(pag.provider_order_id)
            if ordem_atual.status == StatusPagamentoProvedor.CONFIRMADO:
                pag.status = StatusPagamentoProvedor.CONFIRMADO.value
                self._executar_estorno_pago(
                    pag, envelope, traceparent, evento_sucesso=evento_sucesso
                )
                return
            if ordem_atual.status == StatusPagamentoProvedor.PENDENTE:
                ordem_atual = self.provedor.cancelar(
                    pag.provider_order_id, idempotency_key=idem_cancel
                )
            op.estado = "SUCESSO"
            pag.status = "CANCELADO"
            self._publicar_outbox(
                event_type=evento_sucesso,
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
                    "resultado": (
                        "CANCELADO_SEM_COBRANCA"
                        if evento_sucesso == EVT_PAGAMENTO_ESTORNADO
                        else "CANCELADA"
                    ),
                    "motivo": str(p["motivo"]),
                },
                traceparent=traceparent,
            )
        except (ErroComunicacaoProvedorError, ErroOperacaoProvedorError) as exc:
            op.estado = "DESCONHECIDO"
            pag.status = "CANCELAMENTO_PENDENTE"
            self._publicar_outbox(
                event_type=EVT_PAGAMENTO_ESTORNO_PENDENTE,
                saga_id=envelope.saga_id,
                os_id=envelope.os_id,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.event_id,
                aggregate_version=4,
                payload={
                    "rodada": int(p["rodada"]),
                    "compensacao_id": str(p["compensacao_id"]),
                    "pagamento_id": pag.id,
                    "resultado": "PENDENTE_RECONCILIACAO",
                    "motivo": str(exc),
                },
                traceparent=traceparent,
            )

    def _processar_estornar_ou_cancelar_cobranca(
        self, envelope: Any, traceparent: str | None, *, evento_sucesso: str
    ) -> None:
        p = envelope.payload
        self.repo.registrar_tombstone_barreira(
            saga_id=envelope.saga_id,
            os_id=envelope.os_id,
            rodada=int(p["rodada"]),
            execucao_cancelada_event_id=UUID(str(p["execucao_cancelada_event_id"])),
            motivo=str(p["motivo"]),
        )
        pag = self.repo.buscar_pagamento_por_id(UUID(str(p["pagamento_id"])))
        if pag is None:
            self.db.commit()
            return

        if pag.status in {
            "CONFIRMADO",
            "FALHA_ESTORNO",
            "ESTORNO_PENDENTE",
        }:
            if pag.provider_order_id:
                self._executar_estorno_pago(
                    pag, envelope, traceparent, evento_sucesso=evento_sucesso
                )
        else:
            self._executar_cancelamento_nao_pago(
                pag, envelope, traceparent, evento_sucesso=evento_sucesso
            )
        self.db.commit()

    def processar_comando(
        self,
        dados: dict[str, Any],
        *,
        consumer: str = "billing-comandos",
        traceparent: str | None = None,
    ) -> str:
        envelope = validar_comando_billing_v1(dados)
        if self._ja_processado_inbox(consumer, envelope):
            return "DUPLICADO"

        evt = envelope.event_type
        if evt == "CriarCobranca.v1":
            return self._processar_criar_cobranca(
                envelope, traceparent, consumer=consumer
            )

        if not self._registrar_inbox(consumer, envelope):
            return "DUPLICADO"

        if evt == "GerarOrcamento.v1":
            return self._processar_gerar_orcamento(envelope, traceparent)
        if evt == "CancelarOrcamento.v1":
            return self._processar_cancelar_orcamento(envelope, traceparent)
        if evt == "EstornarPagamento.v1":
            self._processar_estornar_ou_cancelar_cobranca(
                envelope, traceparent, evento_sucesso=EVT_PAGAMENTO_ESTORNADO
            )
            return "PROCESSADO"
        if evt == "CancelarCobranca.v1":
            self._processar_estornar_ou_cancelar_cobranca(
                envelope, traceparent, evento_sucesso=EVT_COBRANCA_CANCELADA
            )
            return "PROCESSADO"
        self.db.commit()
        return "PROCESSADO"

    def _reconciliar_create_order(
        self, op: ProviderOperationModel, pag: PagamentoModel
    ) -> int:
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
        if ordem.status == StatusPagamentoProvedor.CONFIRMADO:
            pag.status = StatusPagamentoProvedor.PENDENTE.value
            confirmar_pagamento_aprovado(
                self.db, pag, ordem, environment=self.environment
            )
        return 1

    def _reconciliar_refund(
        self, op: ProviderOperationModel, pag: PagamentoModel
    ) -> int:
        if not pag.provider_order_id:
            return 0
        try:
            ordem = self.provedor.consultar(pag.provider_order_id)
            if ordem.status != StatusPagamentoProvedor.ESTORNADO:
                ordem = self.provedor.estornar(
                    pag.provider_order_id, idempotency_key=op.idempotency_key
                )
        except (ErroComunicacaoProvedorError, ErroOperacaoProvedorError):
            return 0
        if ordem.status != StatusPagamentoProvedor.ESTORNADO:
            return 0
        op.estado = "SUCESSO"
        pag.status = "ESTORNADO"
        self._publicar_outbox(
            event_type=EVT_PAGAMENTO_ESTORNADO,
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
        return 1

    def _reconciliar_cancel(
        self, op: ProviderOperationModel, pag: PagamentoModel
    ) -> int:
        if not pag.provider_order_id:
            return 0
        try:
            ordem = self.provedor.consultar(pag.provider_order_id)
            if ordem.status == StatusPagamentoProvedor.PENDENTE:
                ordem = self.provedor.cancelar(
                    pag.provider_order_id, idempotency_key=op.idempotency_key
                )
        except (ErroComunicacaoProvedorError, ErroOperacaoProvedorError):
            return 0
        if ordem.status not in {
            StatusPagamentoProvedor.CANCELADO,
            StatusPagamentoProvedor.RECUSADO,
        }:
            return 0
        op.estado = "SUCESSO"
        pag.status = "CANCELADO"
        self._publicar_outbox(
            event_type=EVT_PAGAMENTO_ESTORNADO,
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
                "resultado": "CANCELADO_SEM_COBRANCA",
                "motivo": "RECONCILIACAO_CANCELAMENTO",
            },
        )
        return 1

    def _reconciliar_pagamento_pendente(self, pag: PagamentoModel) -> int:
        if not pag.provider_order_id:
            return 0
        try:
            ordem = self.provedor.consultar(pag.provider_order_id)
        except ErroComunicacaoProvedorError:
            return 0
        if ordem.status == StatusPagamentoProvedor.CONFIRMADO:
            if confirmar_pagamento_aprovado(
                self.db, pag, ordem, environment=self.environment
            ):
                return 1
            return 0
        if ordem.status in {
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
            return 1
        return 0

    def reconciliar_operacoes_pendentes(self) -> int:
        """
        Reconcilia operações em estado DESCONHECIDO (timeout de rede/5xx) ou pagamentos
        em PENDENTE_RECONCILIACAO consultando o provedor sem duplicar cobranças ou estornos.
        """
        reconciliadas = 0
        ops = (
            self.db.query(ProviderOperationModel)
            .filter(ProviderOperationModel.estado.in_(["PENDENTE", "DESCONHECIDO"]))
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
                reconciliadas += self._reconciliar_create_order(op, pag)
            elif op.tipo == "REFUND":
                reconciliadas += self._reconciliar_refund(op, pag)
            elif op.tipo == "CANCEL_ORDER":
                reconciliadas += self._reconciliar_cancel(op, pag)

        pags_rec = (
            self.db.query(PagamentoModel)
            .filter(PagamentoModel.status == "PENDENTE_RECONCILIACAO")
            .all()
        )
        for pag in pags_rec:
            reconciliadas += self._reconciliar_pagamento_pendente(pag)

        if reconciliadas > 0:
            self.db.commit()
        return reconciliadas
