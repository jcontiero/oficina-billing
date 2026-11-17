from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from uuid import UUID, uuid4


class StatusOrcamento(str, Enum):
    GERADO = "GERADO"
    APROVADO = "APROVADO"
    RECUSADO = "RECUSADO"
    CANCELADO = "CANCELADO"


class TipoItemOrcamento(str, Enum):
    SERVICO = "SERVICO"
    PECA = "PECA"


@dataclass(frozen=True)
class ItemOrcamento:
    id: UUID
    tipo: TipoItemOrcamento
    referencia_id: UUID
    descricao: str
    quantidade: int
    preco_unitario_centavos: int

    def __post_init__(self) -> None:
        if self.quantidade <= 0:
            raise ValueError("quantidade do item deve ser maior que zero")
        if self.preco_unitario_centavos < 0:
            raise ValueError("preco_unitario_centavos não pode ser negativo")

    @property
    def subtotal_centavos(self) -> int:
        return self.quantidade * self.preco_unitario_centavos


@dataclass
class Orcamento:
    id: UUID
    os_id: UUID
    saga_id: UUID
    cliente_id: UUID
    rodada: int
    versao: int
    itens: list[ItemOrcamento]
    moeda: str = "BRL"
    status: StatusOrcamento = StatusOrcamento.GERADO
    motivo_decisao: str | None = None
    decidido_em: datetime | None = None
    criado_em: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if self.rodada < 1 or self.versao < 1:
            raise ValueError("rodada e versao devem ser >= 1")
        if not self.itens:
            raise ValueError("Orçamento deve possuir pelo menos um item")

    @property
    def total_centavos(self) -> int:
        return sum(item.subtotal_centavos for item in self.itens)

    def registrar_decisao(
        self,
        *,
        cliente_id_solicitante: UUID,
        is_admin: bool,
        versao_informada: int,
        aprovado: bool,
        motivo: str | None = None,
    ) -> None:
        if not is_admin and cliente_id_solicitante != self.cliente_id:
            raise PermissionError(
                "Apenas o cliente dono da OS pode decidir o orçamento"
            )
        if versao_informada != self.versao:
            raise ValueError(
                f"Versão divergente: orçamento atual={self.versao}, enviada={versao_informada}"
            )
        if self.status != StatusOrcamento.GERADO:
            raise ValueError(
                f"Orçamento já se encontra em estado terminal: {self.status.value}"
            )

        self.status = StatusOrcamento.APROVADO if aprovado else StatusOrcamento.RECUSADO
        self.motivo_decisao = motivo
        self.decidido_em = datetime.now(UTC)


def criar_orcamento_congelado(
    *,
    os_id: UUID,
    saga_id: UUID,
    cliente_id: UUID,
    rodada: int,
    versao: int,
    itens: list[ItemOrcamento],
) -> Orcamento:
    return Orcamento(
        id=uuid4(),
        os_id=os_id,
        saga_id=saga_id,
        cliente_id=cliente_id,
        rodada=rodada,
        versao=versao,
        itens=list(itens),
    )
