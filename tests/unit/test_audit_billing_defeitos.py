from datetime import datetime, timezone
import json
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.mensageria.consumidor import BillingConsumidor
from src.mensageria.outbox_inbox import InboxEventoModel, OutboxEventoModel
from src.orcamentos.infraestrutura.modelos import (
    OrcamentoModel,
    PagamentoModel,
    ProviderOperationModel,
)
from src.pagamentos.infraestrutura.mercadopago import (
    ErroComunicacaoProvedorError,
    ErroDivergenciaConfirmacaoError,
    FakeMercadoPagoOrdersProvider,
    MercadoPagoOrdersClient,
    StatusPagamentoProvedor,
)
from src.shared.banco import Base


def _session(tmp_path: Path):
    eng = create_engine(
        f"sqlite:///{tmp_path / 'audit_billing.db'}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(eng)
    return sessionmaker(autocommit=False, autoflush=False, bind=eng)


def _cmd(event_type: str, *, saga_id, os_id, payload: dict) -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "producer": "oficina-os",
        "saga_id": str(saga_id),
        "os_id": str(os_id),
        "correlation_id": str(saga_id),
        "causation_id": str(uuid4()),
        "aggregate_version": 1,
        "payload": payload,
    }


def test_r01_atomicidade_gerar_orcamento_sem_commit_parcial(tmp_path: Path) -> None:
    Session = _session(tmp_path)
    provedor = FakeMercadoPagoOrdersProvider()
    cmd = _cmd(
        "GerarOrcamento.v1",
        saga_id=uuid4(),
        os_id=uuid4(),
        payload={
            "rodada": 1,
            "versao": 1,
            "cliente_id": str(uuid4()),
            "itens_servico": [
                {
                    "servico_id": str(uuid4()),
                    "descricao": "Alinhamento",
                    "preco_unitario_centavos": 10000,
                }
            ],
            "itens_peca": [],
        },
    )
    with Session() as db:
        cons = BillingConsumidor(db, provedor)
        with patch.object(
            cons, "_publicar_outbox", side_effect=RuntimeError("falha outbox")
        ):
            with pytest.raises(RuntimeError):
                cons.processar_comando(cmd)
        db.rollback()

    with Session() as db:
        assert db.query(InboxEventoModel).count() == 0
        assert db.query(OrcamentoModel).count() == 0
        assert db.query(OutboxEventoModel).count() == 0


def test_r03_nao_confirma_cancelamento_quando_provedor_indisponivel(
    tmp_path: Path,
) -> None:
    Session = _session(tmp_path)
    provedor = FakeMercadoPagoOrdersProvider()
    saga_id = uuid4()
    os_id = uuid4()

    with Session() as db:
        cons = BillingConsumidor(db, provedor)
        cons.processar_comando(
            _cmd(
                "CriarCobranca.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "orcamento_id": str(uuid4()),
                    "orcamento_versao": 1,
                    "cliente_id": str(uuid4()),
                    "total_centavos": 15000,
                    "moeda": "BRL",
                },
            )
        )
        pag = db.query(PagamentoModel).one()
        pag_id = pag.id

    provedor.configurar_falha(ErroComunicacaoProvedorError("mp indisponivel"))
    with patch.object(
        provedor,
        "cancelar",
        side_effect=ErroComunicacaoProvedorError("mp indisponivel no cancel"),
    ):
        with Session() as db:
            BillingConsumidor(db, provedor).processar_comando(
                _cmd(
                    "EstornarPagamento.v1",
                    saga_id=saga_id,
                    os_id=os_id,
                    payload={
                        "rodada": 1,
                        "compensacao_id": str(uuid4()),
                        "motivo": "PAGAMENTO_RECUSADO",
                        "pagamento_id": pag_id,
                        "total_centavos": 15000,
                        "moeda": "BRL",
                        "execucao_cancelada_event_id": str(uuid4()),
                    },
                )
            )
            pag = db.query(PagamentoModel).one()
            assert pag.status != "CANCELADO"
            evts = [e.tipo_evento for e in db.query(OutboxEventoModel).all()]
            assert "PagamentoEstornado.v1" not in evts


def test_r09_reconciliacao_valida_divergencia_e_preserva_itens_orcamento(
    tmp_path: Path,
) -> None:
    Session = _session(tmp_path)
    provedor = FakeMercadoPagoOrdersProvider()
    saga_id = uuid4()
    os_id = uuid4()
    srv_id = str(uuid4())

    with Session() as db:
        cons = BillingConsumidor(db, provedor)
        cons.processar_comando(
            _cmd(
                "GerarOrcamento.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "versao": 2,
                    "cliente_id": str(uuid4()),
                    "itens_servico": [
                        {
                            "servico_id": srv_id,
                            "descricao": "Revisao",
                            "preco_unitario_centavos": 20000,
                        }
                    ],
                    "itens_peca": [],
                },
            )
        )
        orc = db.query(OrcamentoModel).one()
        cons.processar_comando(
            _cmd(
                "CriarCobranca.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "orcamento_id": orc.id,
                    "orcamento_versao": 2,
                    "cliente_id": orc.cliente_id,
                    "total_centavos": 20000,
                    "moeda": "BRL",
                },
            )
        )
        pag = db.query(PagamentoModel).one()
        pag.status = "PENDENTE_RECONCILIACAO"
        db.commit()
        provedor.simular_pagamento_aprovado(pag.provider_order_id)

    with Session() as db:
        assert BillingConsumidor(db, provedor).reconciliar_operacoes_pendentes() == 1
        out_conf = (
            db.query(OutboxEventoModel)
            .filter(OutboxEventoModel.tipo_evento == "PagamentoConfirmado.v1")
            .one()
        )
        assert out_conf.payload["payload"]["orcamento_versao"] == 2
        assert len(out_conf.payload["payload"]["itens_servico"]) == 1

        # Divergência de valor deve falhar na validação
        pag = db.query(PagamentoModel).one()
        pag.status = "PENDENTE_RECONCILIACAO"
        pag.total_centavos = 99999
        db.commit()
        cons_rec = BillingConsumidor(db, provedor)
        with pytest.raises(ErroDivergenciaConfirmacaoError):
            cons_rec.reconciliar_operacoes_pendentes()


def test_r10_cancelar_cobranca_v1_tratado(tmp_path: Path) -> None:
    Session = _session(tmp_path)
    provedor = FakeMercadoPagoOrdersProvider()
    saga_id = uuid4()
    os_id = uuid4()
    with Session() as db:
        cons = BillingConsumidor(db, provedor)
        cons.processar_comando(
            _cmd(
                "CriarCobranca.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "orcamento_id": str(uuid4()),
                    "orcamento_versao": 1,
                    "cliente_id": str(uuid4()),
                    "total_centavos": 10000,
                    "moeda": "BRL",
                },
            )
        )
        pag = db.query(PagamentoModel).one()
        cons.processar_comando(
            _cmd(
                "CancelarCobranca.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "compensacao_id": str(uuid4()),
                    "motivo": "ORCAMENTO_RECUSADO",
                    "pagamento_id": pag.id,
                    "execucao_cancelada_event_id": str(uuid4()),
                },
            )
        )
        evts = [e.tipo_evento for e in db.query(OutboxEventoModel).all()]
        assert "CobrancaCancelada.v1" in evts


def test_r12_contrato_mp_external_pos_id_e_busca_exata() -> None:
    captured: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/orders":
            body = json.loads(request.content.decode("utf-8"))
            captured.append(body)
            return httpx.Response(
                201,
                json={
                    "id": "ORD_1",
                    "external_reference": body["external_reference"],
                    "status": "created",
                    "total_amount": body["total_amount"],
                    "currency": "BRL",
                },
            )
        if request.method == "GET" and request.url.path == "/v1/orders":
            return httpx.Response(
                200,
                json={
                    "elements": [
                        {
                            "id": "ORD_OUTRO",
                            "external_reference": "pag-errado",
                            "status": "created",
                            "total_amount": "50.00",
                            "currency": "BRL",
                        },
                        {
                            "id": "ORD_ALVO",
                            "external_reference": "pag-alvo",
                            "status": "created",
                            "total_amount": "100.00",
                            "currency": "BRL",
                        },
                    ]
                },
            )
        return httpx.Response(404)

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://api.mercadopago.com",
    )
    mp = MercadoPagoOrdersClient(
        access_token="tok", external_pos_id="POS_EXT_01", http_client=client
    )
    mp.criar_cobranca(
        pagamento_id="pag-alvo",
        total_centavos=10000,
        moeda="BRL",
        descricao="OS 1",
        idempotency_key="k1",
    )
    assert captured[0]["config"]["qr"]["external_pos_id"] == "POS_EXT_01"
    encontrada = mp.buscar_por_external_reference("pag-alvo")
    assert encontrada is not None
    assert encontrada.provider_order_id == "ORD_ALVO"
    assert encontrada.status == StatusPagamentoProvedor.PENDENTE


def test_r03_reconciliar_cancelamento_pendente_emite_cobranca_cancelada(
    tmp_path: Path,
) -> None:
    Session = _session(tmp_path)
    provedor = FakeMercadoPagoOrdersProvider()
    saga_id = uuid4()
    os_id = uuid4()
    with Session() as db:
        cons = BillingConsumidor(db, provedor)
        cons.processar_comando(
            _cmd(
                "CriarCobranca.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "orcamento_id": str(uuid4()),
                    "orcamento_versao": 1,
                    "cliente_id": str(uuid4()),
                    "total_centavos": 12000,
                    "moeda": "BRL",
                },
            )
        )
        pag = db.query(PagamentoModel).one()
        pag_id = pag.id

    provedor.configurar_falha(ErroComunicacaoProvedorError("timeout cancel"))
    with Session() as db:
        BillingConsumidor(db, provedor).processar_comando(
            _cmd(
                "CancelarCobranca.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "compensacao_id": str(uuid4()),
                    "motivo": "TIMEOUT",
                    "pagamento_id": pag_id,
                    "execucao_cancelada_event_id": str(uuid4()),
                },
            )
        )
        pag = db.query(PagamentoModel).one()
        assert pag.status == "CANCELAMENTO_PENDENTE"
        op = (
            db.query(ProviderOperationModel)
            .filter(ProviderOperationModel.tipo == "CANCEL_ORDER")
            .one()
        )
        assert op.estado == "DESCONHECIDO"

    with Session() as db:
        assert BillingConsumidor(db, provedor).reconciliar_operacoes_pendentes() == 1
        pag = db.query(PagamentoModel).one()
        assert pag.status == "CANCELADO"
        evts = [e.tipo_evento for e in db.query(OutboxEventoModel).all()]
        assert "PagamentoEstornoPendente.v1" in evts
        assert "PagamentoEstornado.v1" in evts
