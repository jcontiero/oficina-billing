from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

PRODUTORES_PERMITIDOS = {"oficina-os", "oficina-billing", "oficina-execucao"}

COMANDOS_COM_BARREIRA_OBRIGATORIA = {
    "LiberarReserva.v1",
    "CancelarOrcamento.v1",
    "CancelarCobranca.v1",
    "EstornarPagamento.v1",
}

COMANDOS_BILLING_V1: dict[str, set[str]] = {
    "GerarOrcamento.v1": {
        "rodada",
        "versao",
        "cliente_id",
        "itens_servico",
        "itens_peca",
    },
    "CriarCobranca.v1": {
        "rodada",
        "orcamento_id",
        "orcamento_versao",
        "cliente_id",
        "total_centavos",
        "moeda",
    },
    "CancelarOrcamento.v1": {
        "rodada",
        "motivo",
        "compensacao_id",
        "orcamento_id",
        "orcamento_versao",
        "execucao_cancelada_event_id",
    },
    "CancelarCobranca.v1": {
        "rodada",
        "motivo",
        "compensacao_id",
        "execucao_cancelada_event_id",
    },
    "EstornarPagamento.v1": {
        "rodada",
        "motivo",
        "compensacao_id",
        "pagamento_id",
        "total_centavos",
        "moeda",
        "execucao_cancelada_event_id",
    },
}

EVENTOS_BILLING_V1: dict[str, set[str]] = {
    "OrcamentoGerado.v1": {
        "rodada",
        "orcamento_id",
        "versao",
        "total_centavos",
        "moeda",
    },
    "OrcamentoAprovado.v1": {
        "rodada",
        "orcamento_id",
        "versao",
        "cliente_id",
        "total_centavos",
        "moeda",
    },
    "OrcamentoRecusado.v1": {
        "rodada",
        "orcamento_id",
        "versao",
        "cliente_id",
        "motivo",
    },
    "CobrancaCriada.v1": {
        "rodada",
        "orcamento_id",
        "pagamento_id",
        "provider_order_id",
        "qr_data",
        "total_centavos",
        "moeda",
    },
    "PagamentoConfirmado.v1": {
        "orcamento_id",
        "pagamento_id",
        "provider_order_id",
        "total_centavos",
        "moeda",
        "orcamento_versao",
    },
    "PagamentoRecusado.v1": {
        "rodada",
        "orcamento_id",
        "pagamento_id",
        "motivo",
    },
    "OrcamentoCancelado.v1": {
        "rodada",
        "compensacao_id",
        "orcamento_id",
        "resultado",
        "motivo",
    },
    "CobrancaCancelada.v1": {
        "rodada",
        "compensacao_id",
        "resultado",
        "motivo",
    },
    "PagamentoEstornado.v1": {
        "rodada",
        "compensacao_id",
        "pagamento_id",
        "provider_order_id",
        "total_centavos",
        "moeda",
        "resultado",
        "motivo",
    },
    "PagamentoEstornoPendente.v1": {
        "rodada",
        "compensacao_id",
        "pagamento_id",
        "resultado",
        "motivo",
    },
    "PagamentoEstornoFalhou.v1": {
        "rodada",
        "compensacao_id",
        "pagamento_id",
        "resultado",
        "motivo",
    },
}

CAMPOS_SENSIVEIS_PROIBIDOS = {
    "cpf",
    "senha",
    "password",
    "secret",
    "access_token",
    "token",
}


class EnvelopeMensagemV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: str
    schema_version: int = Field(default=1)
    occurred_at: datetime
    producer: str
    saga_id: UUID
    os_id: UUID
    correlation_id: UUID
    causation_id: UUID
    aggregate_version: int = Field(ge=1)
    payload: dict[str, Any]

    @field_validator("schema_version")
    @classmethod
    def _validar_versao(cls, valor: int) -> int:
        if valor != 1:
            raise ValueError("schema_version deve ser 1")
        return valor

    @field_validator("producer")
    @classmethod
    def _validar_produtor(cls, valor: str) -> str:
        if valor not in PRODUTORES_PERMITIDOS:
            raise ValueError(f"Produtor não permitido: {valor}")
        return valor


def validar_comando_billing_v1(dados: dict[str, Any]) -> EnvelopeMensagemV1:
    """
    Valida os comandos recebidos pelo serviço de Billing, exigindo obrigatoriamente
    a comprovação da barreira de cancelamento da Execução (execucao_cancelada_event_id)
    antes de cancelar orçamento, cancelar cobrança ou estornar pagamento.
    """
    envelope = EnvelopeMensagemV1.model_validate(dados)
    if envelope.producer != "oficina-os":
        raise ValueError(
            "Comandos de faturamento só podem ser produzidos por oficina-os"
        )
    if envelope.event_type not in COMANDOS_BILLING_V1:
        raise ValueError(f"Comando desconhecido para Billing: {envelope.event_type}")

    obrigatorios = COMANDOS_BILLING_V1[envelope.event_type]
    faltantes = obrigatorios - set(envelope.payload.keys())
    if faltantes:
        raise ValueError(
            f"Payload de {envelope.event_type} sem campos obrigatórios: {sorted(faltantes)}"
        )

    if envelope.event_type in COMANDOS_COM_BARREIRA_OBRIGATORIA:
        barreira_id = envelope.payload.get("execucao_cancelada_event_id")
        if not barreira_id:
            raise ValueError(
                f"Comando {envelope.event_type} rejeitado: exige execucao_cancelada_event_id "
                "comprovando a confirmação prévia de CancelarExecucao.v1"
            )
        UUID(str(barreira_id))

    proibidos = set(envelope.payload.keys()) & CAMPOS_SENSIVEIS_PROIBIDOS
    if proibidos:
        raise ValueError(
            f"Payload contém campos sensíveis proibidos: {sorted(proibidos)}"
        )

    return envelope
