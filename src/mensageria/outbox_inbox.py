from datetime import datetime, timezone
import uuid

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)

from src.shared.banco import Base


class OutboxEventoModel(Base):
    __tablename__ = "outbox_eventos"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    event_id = Column(String(36), unique=True, nullable=False)
    topic = Column(String(128), nullable=False)
    tipo_evento = Column(String(100), nullable=False)
    payload = Column(JSON, nullable=False)
    traceparent = Column(String(128), nullable=True)
    status = Column(String(24), nullable=False, default="PENDENTE")
    tentativas = Column(Integer, nullable=False, default=0)
    next_attempt_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    locked_until = Column(DateTime(timezone=True), nullable=True)
    criado_em = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    processado_em = Column(DateTime(timezone=True), nullable=True)
    erro = Column(Text, nullable=True)


class InboxEventoModel(Base):
    __tablename__ = "inbox_eventos"
    __table_args__ = (
        UniqueConstraint(
            "consumer", "event_id", name="uq_billing_inbox_consumer_event"
        ),
    )

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    consumer = Column(String(64), nullable=False)
    event_id = Column(String(36), nullable=False)
    event_type = Column(String(100), nullable=False)
    saga_id = Column(String(36), nullable=True)
    recebido_em = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
