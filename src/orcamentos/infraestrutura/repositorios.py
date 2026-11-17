from uuid import UUID

from sqlalchemy.orm import Session

from src.orcamentos.dominio.entidades import (
    ItemOrcamento,
    Orcamento,
    StatusOrcamento,
    TipoItemOrcamento,
)
from src.orcamentos.infraestrutura.modelos import (
    OrcamentoItemModel,
    OrcamentoModel,
    PagamentoModel,
    SagaTombstoneBillingModel,
)


class OrcamentoRepositorioSql:
    def __init__(self, db: Session) -> None:
        self.db = db

    def salvar(self, orcamento: Orcamento) -> Orcamento:
        existente = (
            self.db.query(OrcamentoModel)
            .filter(OrcamentoModel.id == str(orcamento.id))
            .first()
        )
        if existente is None:
            model = OrcamentoModel(
                id=str(orcamento.id),
                os_id=str(orcamento.os_id),
                saga_id=str(orcamento.saga_id),
                cliente_id=str(orcamento.cliente_id),
                rodada=orcamento.rodada,
                versao=orcamento.versao,
                status=orcamento.status.value,
                total_centavos=orcamento.total_centavos,
                moeda=orcamento.moeda,
                motivo_decisao=orcamento.motivo_decisao,
                decidido_em=orcamento.decidido_em,
                criado_em=orcamento.criado_em,
                itens=[
                    OrcamentoItemModel(
                        id=str(item.id),
                        tipo=item.tipo.value,
                        referencia_id=str(item.referencia_id),
                        descricao=item.descricao,
                        quantidade=item.quantidade,
                        preco_unitario_centavos=item.preco_unitario_centavos,
                        subtotal_centavos=item.subtotal_centavos,
                    )
                    for item in orcamento.itens
                ],
            )
            self.db.add(model)
        else:
            existente.status = orcamento.status.value
            existente.motivo_decisao = orcamento.motivo_decisao
            existente.decidido_em = orcamento.decidido_em
        self.db.commit()
        return orcamento

    def buscar_por_id(self, orcamento_id: UUID) -> Orcamento | None:
        model = (
            self.db.query(OrcamentoModel)
            .filter(OrcamentoModel.id == str(orcamento_id))
            .first()
        )
        if model is None:
            return None
        return Orcamento(
            id=UUID(model.id),
            os_id=UUID(model.os_id),
            saga_id=UUID(model.saga_id),
            cliente_id=UUID(model.cliente_id),
            rodada=model.rodada,
            versao=model.versao,
            status=StatusOrcamento(model.status),
            moeda=model.moeda,
            motivo_decisao=model.motivo_decisao,
            decidido_em=model.decidido_em,
            criado_em=model.criado_em,
            itens=[
                ItemOrcamento(
                    id=UUID(item.id),
                    tipo=TipoItemOrcamento(item.tipo),
                    referencia_id=UUID(item.referencia_id),
                    descricao=item.descricao,
                    quantidade=item.quantidade,
                    preco_unitario_centavos=item.preco_unitario_centavos,
                )
                for item in model.itens
            ],
        )

    def buscar_pagamento_por_id(self, pagamento_id: UUID) -> PagamentoModel | None:
        return (
            self.db.query(PagamentoModel)
            .filter(PagamentoModel.id == str(pagamento_id))
            .first()
        )

    def buscar_tombstone_por_saga(
        self, saga_id: UUID
    ) -> SagaTombstoneBillingModel | None:
        return (
            self.db.query(SagaTombstoneBillingModel)
            .filter(SagaTombstoneBillingModel.saga_id == str(saga_id))
            .first()
        )

    def registrar_tombstone_barreira(
        self,
        *,
        saga_id: UUID,
        os_id: UUID,
        rodada: int,
        execucao_cancelada_event_id: UUID,
        motivo: str,
    ) -> SagaTombstoneBillingModel:
        existente = self.buscar_tombstone_por_saga(saga_id)
        if existente is not None:
            if existente.execucao_cancelada_event_id != str(
                execucao_cancelada_event_id
            ):
                raise ValueError(
                    "Conflito de barreira: execucao_cancelada_event_id diverge do tombstone persistido"
                )
            return existente

        novo = SagaTombstoneBillingModel(
            saga_id=str(saga_id),
            os_id=str(os_id),
            rodada=rodada,
            execucao_cancelada_event_id=str(execucao_cancelada_event_id),
            motivo=motivo,
        )
        self.db.add(novo)
        self.db.commit()
        return novo
