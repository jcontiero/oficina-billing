"""Provedor de teste compartilhado, com estado e idempotência próprios."""

from dataclasses import asdict

from sqlalchemy import Column, JSON, String, create_engine, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.shared.banco import Base
from src.pagamentos.infraestrutura.mercadopago import (
    FakeMercadoPagoOrdersProvider,
    ResultadoOrdemProvedor,
    StatusPagamentoProvedor,
)


class EstadoProvedorFakeModel(Base):
    __tablename__ = "fake_provider_state"
    id = Column(String(32), primary_key=True)
    estado = Column(JSON, nullable=False)


def _resultado(dados):
    return ResultadoOrdemProvedor(
        **{**dados, "status": StatusPagamentoProvedor(dados["status"])}
    )


class ProvedorFakePersistente:
    """Serializa operações entre API/worker; não consulta o estado do domínio Billing."""

    def __init__(self, database_url: str, webhook_secret: str):
        self.engine = create_engine(database_url)
        self.webhook_secret = webhook_secret

    def __getattr__(self, nome):
        permitidos = {
            "criar_cobranca",
            "consultar",
            "buscar_por_external_reference",
            "cancelar",
            "estornar",
            "simular_pagamento_aprovado",
            "simular_pagamento_recusado",
        }
        if nome not in permitidos:
            raise AttributeError(nome)
        return lambda *args, **kwargs: self._executar(nome, *args, **kwargs)

    def _executar(self, nome, *args, **kwargs):
        tabela = EstadoProvedorFakeModel.__table__
        with self.engine.connect() as conn:
            if self.engine.dialect.name == "sqlite":
                conn.exec_driver_sql("BEGIN IMMEDIATE")
                insert = sqlite_insert
            else:
                conn.begin()
                insert = pg_insert
            conn.execute(
                insert(tabela).values(id="orders", estado={}).on_conflict_do_nothing()
            )
            estado = conn.execute(
                select(tabela.c.estado).where(tabela.c.id == "orders").with_for_update()
            ).scalar_one()
            fake = FakeMercadoPagoOrdersProvider(webhook_secret=self.webhook_secret)
            fake._seq = estado.get("seq", 0)
            fake._orders = {
                k: _resultado(v) for k, v in estado.get("orders", {}).items()
            }
            fake._idempotency_cache = {
                k: _resultado(v) for k, v in estado.get("idempotency", {}).items()
            }
            resultado = getattr(fake, nome)(*args, **kwargs)
            dados = {
                "seq": fake._seq,
                "orders": {k: asdict(v) for k, v in fake._orders.items()},
                "idempotency": {
                    k: asdict(v) for k, v in fake._idempotency_cache.items()
                },
            }
            conn.execute(
                update(tabela).where(tabela.c.id == "orders").values(estado=dados)
            )
            conn.commit()
            return resultado
