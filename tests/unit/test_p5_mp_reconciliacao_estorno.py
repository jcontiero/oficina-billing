import hashlib
import json
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config import Configuracoes
from src.main import criar_app
from src.mensageria.consumidor import BillingConsumidor
from src.mensageria.outbox_inbox import OutboxEventoModel
from src.orcamentos.infraestrutura.modelos import (
    PagamentoModel,
    ProviderOperationModel,
)
from src.pagamentos.infraestrutura.mercadopago import (
    ErroComunicacaoProvedorError,
    ErroOperacaoProvedorError,
    FakeMercadoPagoOrdersProvider,
    MercadoPagoOrdersClient,
    StatusPagamentoProvedor,
)
from src.shared.banco import Base


def _session_factory(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'p5_billing.db'}"
    eng = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(eng)
    return db_url, sessionmaker(autocommit=False, autoflush=False, bind=eng)


def _cmd_envelope(
    event_type: str,
    *,
    saga_id,
    os_id,
    payload: dict,
) -> dict:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "schema_version": 1,
        "occurred_at": "2026-11-17T12:00:00+00:00",
        "producer": "oficina-os",
        "saga_id": str(saga_id),
        "os_id": str(os_id),
        "correlation_id": str(saga_id),
        "causation_id": str(uuid4()),
        "aggregate_version": 1,
        "payload": payload,
    }


def test_mercadopago_orders_provider_http_e_idempotencia() -> None:
    chamadas: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        chamadas.append(
            (request.method, str(request.url), request.headers.get("X-Idempotency-Key"))
        )
        if request.method == "POST" and request.url.path == "/v1/orders":
            body = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                201,
                json={
                    "id": "ORD_MP_123",
                    "external_reference": body["external_reference"],
                    "status": "created",
                    "total_amount": body["total_amount"],
                    "currency": "BRL",
                    "type_response": {"qr_data": "000201QR"},
                },
            )
        if request.method == "GET" and request.url.path == "/v1/orders/ORD_MP_123":
            return httpx.Response(
                200,
                json={
                    "id": "ORD_MP_123",
                    "external_reference": "pag-1",
                    "status": "processed",
                    "total_amount": "150.00",
                    "currency": "BRL",
                },
            )
        if request.method == "GET" and "external_reference=pag-1" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "elements": [
                        {
                            "id": "ORD_MP_123",
                            "external_reference": "pag-1",
                            "status": "created",
                            "total_amount": "150.00",
                            "currency": "BRL",
                        }
                    ]
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/v1/orders/ORD_MP_123/refund"
        ):
            return httpx.Response(
                200,
                json={
                    "id": "ORD_MP_123",
                    "external_reference": "pag-1",
                    "status": "refunded",
                    "total_amount": "150.00",
                    "currency": "BRL",
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/v1/orders/ORD_MP_429/cancel"
        ):
            return httpx.Response(429, text="rate limit")
        return httpx.Response(400, text="bad request")

    client = httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="https://api.mercadopago.com",
    )
    provider = MercadoPagoOrdersClient(
        access_token="APP_USR-test-token",
        external_pos_id="POS_01",
        http_client=client,
    )

    criada = provider.criar_cobranca(
        pagamento_id="pag-1",
        total_centavos=15000,
        moeda="BRL",
        descricao="OS 1",
        idempotency_key="idem-1",
    )
    assert criada.provider_order_id == "ORD_MP_123"
    assert criada.status == StatusPagamentoProvedor.PENDENTE
    assert chamadas[0][2] == "idem-1"

    consultada = provider.consultar("ORD_MP_123")
    assert consultada.status == StatusPagamentoProvedor.CONFIRMADO

    encontrada = provider.buscar_por_external_reference("pag-1")
    assert encontrada is not None
    assert encontrada.provider_order_id == "ORD_MP_123"

    estornada = provider.estornar("ORD_MP_123", idempotency_key="refund-1")
    assert estornada.status == StatusPagamentoProvedor.ESTORNADO

    try:
        provider.cancelar("ORD_MP_429", idempotency_key="cancel-429")
        assert False, "Esperado ErroComunicacaoProvedorError para HTTP 429"
    except ErroComunicacaoProvedorError:
        pass


def test_reconciliacao_timeout_criar_cobranca_sem_duplicar(tmp_path: Path) -> None:
    _, session_factory = _session_factory(tmp_path)
    provedor = FakeMercadoPagoOrdersProvider()
    provedor.configurar_falha(
        ErroComunicacaoProvedorError("timeout apos criar ordem"), apos_efeito=True
    )

    saga_id = uuid4()
    os_id = uuid4()
    cmd = _cmd_envelope(
        "CriarCobranca.v1",
        saga_id=saga_id,
        os_id=os_id,
        payload={
            "rodada": 1,
            "orcamento_id": str(uuid4()),
            "orcamento_versao": 1,
            "cliente_id": str(uuid4()),
            "total_centavos": 25000,
            "moeda": "BRL",
        },
    )

    with session_factory() as db:
        res = BillingConsumidor(db, provedor).processar_comando(cmd)
        assert res == "PENDENTE_RECONCILIACAO"
        op = db.query(ProviderOperationModel).first()
        pag = db.query(PagamentoModel).first()
        assert op is not None
        assert op.estado == "DESCONHECIDO"
        assert pag is not None
        assert pag.status == "PENDENTE_RECONCILIACAO"
        assert db.query(OutboxEventoModel).count() == 0

    with session_factory() as db:
        reconciliadas = BillingConsumidor(
            db, provedor
        ).reconciliar_operacoes_pendentes()
        assert reconciliadas == 1
        op = db.query(ProviderOperationModel).first()
        pag = db.query(PagamentoModel).first()
        out = db.query(OutboxEventoModel).first()
        assert op is not None
        assert op.estado == "SUCESSO"
        assert pag is not None
        expected_id = hashlib.sha256(pag.id.encode()).hexdigest()[:12].upper()
        assert pag.provider_order_id == f"ORD_TEST_{expected_id}"
        assert out is not None
        assert out.tipo_evento == "CobrancaCriada.v1"
        # Não houve segunda criação de ordem no provedor
        assert provedor._seq == 1


def test_fake_order_id_permanece_estavel_apos_perda_do_estado() -> None:
    pagamento_id = str(uuid4())
    kwargs = {
        "pagamento_id": pagamento_id,
        "total_centavos": 25000,
        "moeda": "BRL",
        "descricao": "OS de teste",
        "idempotency_key": f"create-{pagamento_id}-1",
    }

    ordem_original = FakeMercadoPagoOrdersProvider().criar_cobranca(**kwargs)
    ordem_recuperada = FakeMercadoPagoOrdersProvider().criar_cobranca(**kwargs)

    assert ordem_recuperada.provider_order_id == ordem_original.provider_order_id


def test_webhook_assinatura_recusa_e_falha_estorno_recuperavel(tmp_path: Path) -> None:
    db_url, session_factory = _session_factory(tmp_path)
    cfg = Configuracoes(
        database_url=db_url,
        environment="homolog",
        dd_trace_enabled=False,
        mp_provider_mode="fake",
        mp_webhook_secret="test-secret-p5",
    )
    app = criar_app(cfg)
    provedor = FakeMercadoPagoOrdersProvider(webhook_secret="test-secret-p5")
    app.state.provedor_pagamento = provedor

    saga_id = uuid4()
    os_id = uuid4()
    with session_factory() as db:
        BillingConsumidor(db, provedor).processar_comando(
            _cmd_envelope(
                "CriarCobranca.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "orcamento_id": str(uuid4()),
                    "orcamento_versao": 1,
                    "cliente_id": str(uuid4()),
                    "total_centavos": 18000,
                    "moeda": "BRL",
                },
            )
        )
        pag = db.query(PagamentoModel).first()
        assert pag is not None
        pag_id = pag.id
        order_id = pag.provider_order_id

    client = TestClient(app)

    # 1. Assinatura inválida -> 401
    r_inv = client.post(
        "/webhooks/mercadopago",
        headers={"x-signature": "ts=1,v1=invalido", "x-request-id": "req-1"},
        json={"data": {"id": order_id}, "action": "order.processed"},
    )
    assert r_inv.status_code == 401

    # 2. Falha transitória durante webhook -> marca PENDENTE_RECONCILIACAO e reconcilia
    headers_ok, body_ok = provedor.simular_pagamento_aprovado(
        order_id, x_request_id="req-ok-1"
    )
    provedor.configurar_falha(ErroComunicacaoProvedorError("503 temporario"))
    r_wh = client.post("/webhooks/mercadopago", headers=headers_ok, json=body_ok)
    assert r_wh.status_code == 200

    with session_factory() as db:
        pag = db.query(PagamentoModel).filter(PagamentoModel.id == pag_id).first()
        assert pag is not None
        assert pag.status == "PENDENTE_RECONCILIACAO"
        reconciliadas = BillingConsumidor(
            db, provedor
        ).reconciliar_operacoes_pendentes()
        assert reconciliadas == 1
        pag = db.query(PagamentoModel).filter(PagamentoModel.id == pag_id).first()
        assert pag is not None
        assert pag.status == "CONFIRMADO"

    # 3. Falha no estorno -> FALHA_ESTORNO + EstornoFalhou.v1, depois recupera via reconciliação
    provedor.configurar_falha(ErroOperacaoProvedorError("timeout MP refund"))
    with session_factory() as db:
        BillingConsumidor(db, provedor).processar_comando(
            _cmd_envelope(
                "EstornarPagamento.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "compensacao_id": str(uuid4()),
                    "motivo": "FALHA_ENFILEIRAMENTO",
                    "pagamento_id": pag_id,
                    "total_centavos": 18000,
                    "moeda": "BRL",
                    "execucao_cancelada_event_id": str(uuid4()),
                },
            )
        )
        pag = db.query(PagamentoModel).filter(PagamentoModel.id == pag_id).first()
        assert pag is not None
        assert pag.status == "FALHA_ESTORNO"
        evts = [e.tipo_evento for e in db.query(OutboxEventoModel).all()]
        assert "PagamentoEstornoFalhou.v1" in evts

    # Simula timeout recuperável (DESCONHECIDO) e reconciliação posterior sem estorno duplicado
    with session_factory() as db:
        op_ref = (
            db.query(ProviderOperationModel)
            .filter(ProviderOperationModel.tipo == "REFUND")
            .first()
        )
        assert op_ref is not None
        op_ref.estado = "DESCONHECIDO"
        db.commit()

    with session_factory() as db:
        rec_ref = BillingConsumidor(db, provedor).reconciliar_operacoes_pendentes()
        assert rec_ref == 1
        pag = db.query(PagamentoModel).filter(PagamentoModel.id == pag_id).first()
        assert pag is not None
        assert pag.status == "ESTORNADO"


def test_webhook_recusa_pagamento_emite_evento(tmp_path: Path) -> None:
    db_url, session_factory = _session_factory(tmp_path)
    cfg = Configuracoes(
        database_url=db_url,
        environment="homolog",
        dd_trace_enabled=False,
        mp_provider_mode="fake",
        mp_webhook_secret="test-secret-recusa",
    )
    app = criar_app(cfg)
    provedor = FakeMercadoPagoOrdersProvider(webhook_secret="test-secret-recusa")
    app.state.provedor_pagamento = provedor

    saga_id = uuid4()
    os_id = uuid4()
    with session_factory() as db:
        BillingConsumidor(db, provedor).processar_comando(
            _cmd_envelope(
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
        pag = db.query(PagamentoModel).first()
        assert pag is not None
        order_id = pag.provider_order_id

    headers_rec, body_rec = provedor.simular_pagamento_recusado(order_id)
    r = TestClient(app).post(
        "/webhooks/mercadopago", headers=headers_rec, json=body_rec
    )
    assert r.status_code == 200

    with session_factory() as db:
        pag = db.query(PagamentoModel).first()
        assert pag is not None
        assert pag.status == "RECUSADO"
        evts = [e.tipo_evento for e in db.query(OutboxEventoModel).all()]
        assert "PagamentoRecusado.v1" in evts
