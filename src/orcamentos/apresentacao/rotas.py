import json
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.orcamentos.infraestrutura.modelos import WebhookInboxModel
from src.orcamentos.infraestrutura.repositorios import OrcamentoRepositorioSql
from src.pagamentos.infraestrutura.mercadopago import validar_assinatura_webhook_mp
from src.shared.seguranca import ActorType, Principal, get_principal_atual

router = APIRouter()


class DecisaoOrcamentoRequest(BaseModel):
    aprovado: bool
    versao: int = Field(ge=1)
    motivo: str | None = None


def get_db(request: Request):
    session_factory = request.app.state.session_factory
    db = session_factory()
    try:
        yield db
    finally:
        db.close()


@router.get("/v1/orcamentos/{orcamento_id}", tags=["Orçamentos"])
def consultar_orcamento(
    orcamento_id: UUID,
    principal: Principal = Depends(get_principal_atual),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    repo = OrcamentoRepositorioSql(db)
    orcamento = repo.buscar_por_id(orcamento_id)
    if orcamento is None:
        raise HTTPException(status_code=404, detail="Orçamento não encontrado")

    if (
        principal.actor_type == ActorType.CLIENTE
        and principal.id != orcamento.cliente_id
    ):
        raise HTTPException(
            status_code=403, detail="Acesso negado ao orçamento de outro cliente"
        )

    return {
        "id": str(orcamento.id),
        "os_id": str(orcamento.os_id),
        "saga_id": str(orcamento.saga_id),
        "cliente_id": str(orcamento.cliente_id),
        "rodada": orcamento.rodada,
        "versao": orcamento.versao,
        "status": orcamento.status.value,
        "total_centavos": orcamento.total_centavos,
        "moeda": orcamento.moeda,
        "motivo_decisao": orcamento.motivo_decisao,
        "itens": [
            {
                "id": str(i.id),
                "tipo": i.tipo.value,
                "referencia_id": str(i.referencia_id),
                "descricao": i.descricao,
                "quantidade": i.quantidade,
                "preco_unitario_centavos": i.preco_unitario_centavos,
                "subtotal_centavos": i.subtotal_centavos,
            }
            for i in orcamento.itens
        ],
    }


@router.post(
    "/v1/orcamentos/{orcamento_id}/decisao",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Orçamentos"],
)
def decidir_orcamento(
    orcamento_id: UUID,
    body: DecisaoOrcamentoRequest,
    principal: Principal = Depends(get_principal_atual),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    repo = OrcamentoRepositorioSql(db)
    orcamento = repo.buscar_por_id(orcamento_id)
    if orcamento is None:
        raise HTTPException(status_code=404, detail="Orçamento não encontrado")

    is_admin = (
        principal.actor_type == ActorType.FUNCIONARIO and principal.perfil == "ADMIN"
    )
    try:
        orcamento.registrar_decisao(
            cliente_id_solicitante=principal.id,
            is_admin=is_admin,
            versao_informada=body.versao,
            aprovado=body.aprovado,
            motivo=body.motivo,
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    repo.salvar(orcamento)
    return {
        "orcamento_id": str(orcamento.id),
        "os_id": str(orcamento.os_id),
        "saga_id": str(orcamento.saga_id),
        "versao": orcamento.versao,
        "status": orcamento.status.value,
        "total_centavos": orcamento.total_centavos,
        "moeda": orcamento.moeda,
    }


@router.get("/v1/pagamentos/{pagamento_id}", tags=["Pagamentos"])
def consultar_pagamento(
    pagamento_id: UUID,
    principal: Principal = Depends(get_principal_atual),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    repo = OrcamentoRepositorioSql(db)
    pagamento = repo.buscar_pagamento_por_id(pagamento_id)
    if pagamento is None:
        raise HTTPException(status_code=404, detail="Pagamento não encontrado")

    if (
        principal.actor_type == ActorType.CLIENTE
        and str(principal.id) != pagamento.cliente_id
    ):
        raise HTTPException(
            status_code=403, detail="Acesso negado ao pagamento de outro cliente"
        )

    return {
        "id": pagamento.id,
        "orcamento_id": pagamento.orcamento_id,
        "os_id": pagamento.os_id,
        "saga_id": pagamento.saga_id,
        "provider_order_id": pagamento.provider_order_id,
        "qr_data": pagamento.qr_data,
        "status": pagamento.status,
        "total_centavos": pagamento.total_centavos,
        "moeda": pagamento.moeda,
    }


@router.post("/webhooks/mercadopago", tags=["Pagamentos"])
def receber_webhook_mercadopago(
    request: Request,
    payload: dict[str, Any],
    x_signature: str = Header(default=""),
    x_request_id: str = Header(default=""),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    data_id = str((payload.get("data") or {}).get("id", ""))
    action = str(payload.get("action", "order.updated"))
    webhook_secret = request.app.state.config.mp_webhook_secret

    if not validar_assinatura_webhook_mp(
        x_signature=x_signature,
        x_request_id=x_request_id,
        data_id=data_id,
        webhook_secret=webhook_secret,
    ):
        raise HTTPException(status_code=401, detail="Assinatura x-signature inválida")

    existente = (
        db.query(WebhookInboxModel)
        .filter(WebhookInboxModel.request_id == x_request_id)
        .first()
    )
    if existente is None:
        registro = WebhookInboxModel(
            request_id=x_request_id,
            data_id=data_id,
            action=action,
            payload_json=json.dumps(payload),
        )
        db.add(registro)
        db.commit()

    return {"status": "received", "data_id": data_id}
