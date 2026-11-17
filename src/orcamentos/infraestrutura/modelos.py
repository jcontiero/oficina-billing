import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.shared.banco import Base


class OrcamentoModel(Base):
    __tablename__ = "orcamentos"
    __table_args__ = (
        UniqueConstraint(
            "os_id", "rodada", "versao", name="uq_orcamento_os_rodada_versao"
        ),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    os_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    saga_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    cliente_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    rodada: Mapped[int] = mapped_column(Integer, nullable=False)
    versao: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="GERADO")
    total_centavos: Mapped[int] = mapped_column(BigInteger, nullable=False)
    moeda: Mapped[str] = mapped_column(String(3), nullable=False, default="BRL")
    motivo_decisao: Mapped[str | None] = mapped_column(Text, nullable=True)
    decidido_em: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )

    itens: Mapped[list["OrcamentoItemModel"]] = relationship(
        back_populates="orcamento", cascade="all, delete-orphan"
    )


class OrcamentoItemModel(Base):
    __tablename__ = "orcamento_itens"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    orcamento_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("orcamentos.id", ondelete="CASCADE"), nullable=False
    )
    tipo: Mapped[str] = mapped_column(String(16), nullable=False)
    referencia_id: Mapped[str] = mapped_column(String(36), nullable=False)
    descricao: Mapped[str] = mapped_column(String(255), nullable=False)
    quantidade: Mapped[int] = mapped_column(Integer, nullable=False)
    preco_unitario_centavos: Mapped[int] = mapped_column(BigInteger, nullable=False)
    subtotal_centavos: Mapped[int] = mapped_column(BigInteger, nullable=False)

    orcamento: Mapped[OrcamentoModel] = relationship(back_populates="itens")


class PagamentoModel(Base):
    __tablename__ = "pagamentos"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    orcamento_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("orcamentos.id"), nullable=False, index=True
    )
    os_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    saga_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    cliente_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    rodada: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_order_id: Mapped[str | None] = mapped_column(
        String(128), unique=True, nullable=True
    )
    qr_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDENTE")
    total_centavos: Mapped[int] = mapped_column(BigInteger, nullable=False)
    moeda: Mapped[str] = mapped_column(String(3), nullable=False, default="BRL")
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )


class ProviderOperationModel(Base):
    __tablename__ = "provider_operations"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    pagamento_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    saga_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    idempotency_key: Mapped[str] = mapped_column(
        String(128), unique=True, nullable=False
    )
    tipo: Mapped[str] = mapped_column(String(32), nullable=False)
    estado: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDENTE")
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )


class WebhookInboxModel(Base):
    __tablename__ = "webhook_inbox"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    request_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    data_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    recebido_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
