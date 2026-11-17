"""schema inicial do servico de billing (orcamentos, pagamentos, outbox e inbox)

Revision ID: 20261117_billing_01
Revises:
Create Date: 2026-11-17 16:40:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "20261117_billing_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "orcamentos",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("os_id", sa.String(length=36), nullable=False),
        sa.Column("saga_id", sa.String(length=36), nullable=False),
        sa.Column("cliente_id", sa.String(length=36), nullable=False),
        sa.Column("rodada", sa.Integer(), nullable=False),
        sa.Column("versao", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("total_centavos", sa.Integer(), nullable=False),
        sa.Column("moeda", sa.String(length=8), nullable=False),
        sa.Column("motivo_recusa", sa.Text(), nullable=True),
        sa.Column("criado_em", sa.DateTime(timezone=True), nullable=False),
        sa.Column("atualizado_em", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "os_id", "rodada", "versao", name="uq_orcamento_os_rodada_versao"
        ),
    )
    op.create_index("ix_orcamentos_os_id", "orcamentos", ["os_id"], unique=False)
    op.create_index("ix_orcamentos_saga_id", "orcamentos", ["saga_id"], unique=False)

    op.create_table(
        "orcamento_itens",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("orcamento_id", sa.String(length=36), nullable=False),
        sa.Column("tipo", sa.String(length=16), nullable=False),
        sa.Column("referencia_id", sa.String(length=36), nullable=False),
        sa.Column("descricao", sa.String(length=255), nullable=False),
        sa.Column("quantidade", sa.Integer(), nullable=False),
        sa.Column("preco_unitario_centavos", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["orcamento_id"], ["orcamentos.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "pagamentos",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("orcamento_id", sa.String(length=36), nullable=False),
        sa.Column("os_id", sa.String(length=36), nullable=False),
        sa.Column("saga_id", sa.String(length=36), nullable=False),
        sa.Column("cliente_id", sa.String(length=36), nullable=False),
        sa.Column("rodada", sa.Integer(), nullable=False),
        sa.Column("provider_order_id", sa.String(length=64), nullable=True),
        sa.Column("qr_data", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("total_centavos", sa.Integer(), nullable=False),
        sa.Column("moeda", sa.String(length=8), nullable=False),
        sa.Column("criado_em", sa.DateTime(timezone=True), nullable=False),
        sa.Column("atualizado_em", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider_order_id"),
    )
    op.create_index("ix_pagamentos_os_id", "pagamentos", ["os_id"], unique=False)
    op.create_index("ix_pagamentos_saga_id", "pagamentos", ["saga_id"], unique=False)

    op.create_table(
        "webhook_inbox",
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("data_id", sa.String(length=64), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("recebido_em", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("request_id"),
    )

    op.create_table(
        "saga_tombstones",
        sa.Column("saga_id", sa.String(length=36), nullable=False),
        sa.Column("os_id", sa.String(length=36), nullable=False),
        sa.Column("rodada", sa.Integer(), nullable=False),
        sa.Column("execucao_cancelada_event_id", sa.String(length=36), nullable=False),
        sa.Column("motivo", sa.String(length=64), nullable=False),
        sa.Column("registrado_em", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("saga_id"),
    )

    op.create_table(
        "provider_operations",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("pagamento_id", sa.String(length=36), nullable=False),
        sa.Column("saga_id", sa.String(length=36), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("tipo", sa.String(length=32), nullable=False),
        sa.Column("estado", sa.String(length=32), nullable=False),
        sa.Column("provider_id", sa.String(length=64), nullable=True),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("criado_em", sa.DateTime(timezone=True), nullable=False),
        sa.Column("atualizado_em", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_index(
        "ix_provider_operations_pagamento_id",
        "provider_operations",
        ["pagamento_id"],
        unique=False,
    )
    op.create_index(
        "ix_provider_operations_saga_id",
        "provider_operations",
        ["saga_id"],
        unique=False,
    )

    op.create_table(
        "outbox_eventos",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("topic", sa.String(length=128), nullable=False),
        sa.Column("tipo_evento", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("traceparent", sa.String(length=128), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("tentativas", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("erro", sa.Text(), nullable=True),
        sa.Column("criado_em", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processado_em", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id"),
    )

    op.create_table(
        "inbox_eventos",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("consumer", sa.String(length=64), nullable=False),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("saga_id", sa.String(length=36), nullable=False),
        sa.Column("recebido_em", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "consumer", "event_id", name="uq_inbox_billing_consumer_event"
        ),
    )


def downgrade() -> None:
    op.drop_table("inbox_eventos")
    op.drop_table("outbox_eventos")
    op.drop_index("ix_provider_operations_saga_id", table_name="provider_operations")
    op.drop_index(
        "ix_provider_operations_pagamento_id", table_name="provider_operations"
    )
    op.drop_table("provider_operations")
    op.drop_table("saga_tombstones")
    op.drop_table("webhook_inbox")
    op.drop_index("ix_pagamentos_saga_id", table_name="pagamentos")
    op.drop_index("ix_pagamentos_os_id", table_name="pagamentos")
    op.drop_table("pagamentos")
    op.drop_table("orcamento_itens")
    op.drop_index("ix_orcamentos_saga_id", table_name="orcamentos")
    op.drop_index("ix_orcamentos_os_id", table_name="orcamentos")
    op.drop_table("orcamentos")
