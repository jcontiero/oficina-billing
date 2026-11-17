from datetime import datetime, timezone
import json
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.mensageria.outbox_inbox import OutboxEventoModel
from src.orcamentos.dominio.entidades import (
    ItemOrcamento,
    TipoItemOrcamento,
    criar_orcamento_congelado,
)
from src.orcamentos.infraestrutura.modelos import PagamentoModel, WebhookInboxModel
from src.orcamentos.infraestrutura.repositorios import OrcamentoRepositorioSql
from src.pagamentos.infraestrutura.mercadopago import (
    StatusPagamentoProvedor,
    validar_assinatura_webhook_mp,
    validar_confirmacao_cobranca,
)
from src.shared.seguranca import ActorType, Principal, get_principal_atual

router = APIRouter()


class DecisaoOrcamentoRequest(BaseModel):
    aprovado: bool
    versao: int = Field(ge=1)
    motivo: str | None = None


class ItemOrcamentoRequest(BaseModel):
    tipo: TipoItemOrcamento
    referencia_id: UUID
    descricao: str = Field(min_length=1)
    quantidade: int = Field(ge=1)
    preco_unitario_centavos: int = Field(ge=0)


class CriarOrcamentoRequest(BaseModel):
    os_id: UUID
    saga_id: UUID
    cliente_id: UUID
    rodada: int = Field(default=1, ge=1)
    versao: int = Field(default=1, ge=1)
    itens: list[ItemOrcamentoRequest] = Field(min_length=1)


def get_db(request: Request):
    session_factory = request.app.state.session_factory
    db = session_factory()
    try:
        yield db
    finally:
        db.close()


@router.post(
    "/v1/orcamentos",
    status_code=status.HTTP_201_CREATED,
    tags=["Orçamentos"],
)
def criar_orcamento_endpoint(
    body: CriarOrcamentoRequest,
    principal: Principal = Depends(get_principal_atual),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    if principal.actor_type != ActorType.FUNCIONARIO or principal.perfil not in {
        "ADMIN",
        "MECANICO",
    }:
        raise HTTPException(
            status_code=403, detail="Apenas funcionários podem gerar orçamento"
        )

    repo = OrcamentoRepositorioSql(db)
    if repo.buscar_tombstone_por_saga(body.saga_id) is not None:
        raise HTTPException(
            status_code=409,
            detail="Saga cancelada por tombstone; não é permitido gerar orçamento tardio",
        )

    orcamento = criar_orcamento_congelado(
        os_id=body.os_id,
        saga_id=body.saga_id,
        cliente_id=body.cliente_id,
        rodada=body.rodada,
        versao=body.versao,
        itens=[
            ItemOrcamento(
                id=uuid4(),
                tipo=i.tipo,
                referencia_id=i.referencia_id,
                descricao=i.descricao,
                quantidade=i.quantidade,
                preco_unitario_centavos=i.preco_unitario_centavos,
            )
            for i in body.itens
        ],
    )
    repo.salvar(orcamento)
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
    }


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
    request: Request,
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

    env = request.app.state.config.environment
    evt_id = uuid4()
    if body.aprovado:
        event_type = "OrcamentoAprovado.v1"
        payload_evt = {
            "rodada": orcamento.rodada,
            "orcamento_id": str(orcamento.id),
            "versao": orcamento.versao,
            "cliente_id": str(orcamento.cliente_id),
            "total_centavos": orcamento.total_centavos,
            "moeda": orcamento.moeda,
            "itens_peca": [
                {
                    "peca_id": str(i.referencia_id),
                    "descricao": i.descricao,
                    "quantidade": i.quantidade,
                    "preco_unitario_centavos": i.preco_unitario_centavos,
                }
                for i in orcamento.itens
                if i.tipo == TipoItemOrcamento.PECA
            ],
        }
    else:
        event_type = "OrcamentoRecusado.v1"
        payload_evt = {
            "rodada": orcamento.rodada,
            "orcamento_id": str(orcamento.id),
            "versao": orcamento.versao,
            "cliente_id": str(orcamento.cliente_id),
            "motivo": body.motivo or "RECUSADO_PELO_CLIENTE",
        }

    db.add(
        OutboxEventoModel(
            id=str(evt_id),
            event_id=str(evt_id),
            topic=f"oficina-{env}-eventos-v1",
            tipo_evento=event_type,
            payload={
                "event_id": str(evt_id),
                "event_type": event_type,
                "schema_version": 1,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "producer": "oficina-billing",
                "saga_id": str(orcamento.saga_id),
                "os_id": str(orcamento.os_id),
                "correlation_id": str(orcamento.saga_id),
                "causation_id": str(evt_id),
                "aggregate_version": 2,
                "payload": payload_evt,
            },
            traceparent=request.headers.get("traceparent"),
            status="PENDENTE",
        )
    )
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

        pag = (
            db.query(PagamentoModel)
            .filter(PagamentoModel.provider_order_id == data_id)
            .first()
        )
        if pag is not None and pag.status != StatusPagamentoProvedor.CONFIRMADO.value:
            provedor = request.app.state.provedor_pagamento
            try:
                ordem = provedor.consultar(data_id)
                if ordem.status == StatusPagamentoProvedor.CONFIRMADO:
                    validar_confirmacao_cobranca(
                        ordem,
                        pagamento_id_esperado=pag.id,
                        total_centavos_esperado=pag.total_centavos,
                        moeda_esperada=pag.moeda,
                    )
                    pag.status = StatusPagamentoProvedor.CONFIRMADO.value
                    orc = OrcamentoRepositorioSql(db).buscar_por_id(
                        UUID(pag.orcamento_id)
                    )
                    itens_servico = []
                    itens_peca = []
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
                    env = request.app.state.config.environment
                    db.add(
                        OutboxEventoModel(
                            id=str(evt_id),
                            event_id=str(evt_id),
                            topic=f"oficina-{env}-eventos-v1",
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
                                    "provider_order_id": data_id,
                                    "total_centavos": pag.total_centavos,
                                    "moeda": pag.moeda,
                                    "orcamento_versao": orc.versao if orc else 1,
                                    "itens_servico": itens_servico,
                                    "itens_peca": itens_peca,
                                },
                            },
                            status="PENDENTE",
                        )
                    )
            except Exception:
                pass
        db.commit()

    return {"status": "received", "data_id": data_id}


@router.get("/v1/orcamentos/por-os/{os_id}", tags=["Orçamentos"])
def buscar_orcamento_por_os(
    os_id: UUID,
    principal: Principal = Depends(get_principal_atual),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    from src.orcamentos.infraestrutura.modelos import OrcamentoModel

    orc = (
        db.query(OrcamentoModel)
        .filter(OrcamentoModel.os_id == str(os_id))
        .order_by(OrcamentoModel.rodada.desc(), OrcamentoModel.versao.desc())
        .first()
    )
    if orc is None:
        raise HTTPException(status_code=404, detail="Orçamento não encontrado para OS")
    if (
        principal.actor_type == ActorType.CLIENTE
        and str(principal.id) != orc.cliente_id
    ):
        raise HTTPException(status_code=403, detail="Acesso negado")
    return {
        "id": orc.id,
        "os_id": orc.os_id,
        "saga_id": orc.saga_id,
        "rodada": orc.rodada,
        "versao": orc.versao,
        "status": orc.status,
        "total_centavos": orc.total_centavos,
    }


@router.get("/v1/pagamentos/por-os/{os_id}", tags=["Pagamentos"])
def buscar_pagamento_por_os(
    os_id: UUID,
    principal: Principal = Depends(get_principal_atual),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    pag = (
        db.query(PagamentoModel)
        .filter(PagamentoModel.os_id == str(os_id))
        .order_by(PagamentoModel.rodada.desc())
        .first()
    )
    if pag is None:
        raise HTTPException(status_code=404, detail="Pagamento não encontrado para OS")
    if (
        principal.actor_type == ActorType.CLIENTE
        and str(principal.id) != pag.cliente_id
    ):
        raise HTTPException(status_code=403, detail="Acesso negado")
    return {
        "id": pag.id,
        "orcamento_id": pag.orcamento_id,
        "os_id": pag.os_id,
        "saga_id": pag.saga_id,
        "provider_order_id": pag.provider_order_id,
        "qr_data": pag.qr_data,
        "status": pag.status,
        "total_centavos": pag.total_centavos,
    }


@router.post("/internal/fake-mp/{provider_order_id}/pagar", tags=["Pagamentos"])
def simular_pagamento_fake_e2e(
    provider_order_id: str,
    request: Request,
) -> dict[str, Any]:
    if request.app.state.config.mp_provider_mode != "fake":
        raise HTTPException(
            status_code=404, detail="Disponível apenas quando mp_provider_mode=fake"
        )
    provedor = request.app.state.provedor_pagamento
    headers, body = provedor.simular_pagamento_aprovado(provider_order_id)
    return {"headers": headers, "body": body}
