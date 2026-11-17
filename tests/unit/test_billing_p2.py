import base64
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from src.config import Configuracoes
from src.main import criar_app
from src.mensageria.contratos import validar_comando_billing_v1
from src.orcamentos.dominio.entidades import (
    ItemOrcamento,
    StatusOrcamento,
    TipoItemOrcamento,
    criar_orcamento_congelado,
)
from src.orcamentos.infraestrutura.modelos import PagamentoModel
from src.orcamentos.infraestrutura.repositorios import OrcamentoRepositorioSql
from src.pagamentos.infraestrutura.mercadopago import (
    ErroComunicacaoProvedorError,
    ErroDivergenciaConfirmacaoError,
    ErroOperacaoProvedorError,
    FakeMercadoPagoOrdersProvider,
    MercadoPagoOrdersClient,
    StatusPagamentoProvedor,
    centavos_para_decimal_str,
    decimal_str_para_centavos,
    gerar_assinatura_webhook_mp,
    mapear_status_order_mp,
    validar_confirmacao_cobranca,
)
from src.shared.banco import Base
from src.shared.observabilidade import configurar_observabilidade, correlacao_ctx


def _gerar_par_rsa_e_jwks(kid: str = "test-kid-1"):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_numbers = private_key.public_key().public_numbers()

    def _b64(n: int) -> str:
        b = n.to_bytes((n.bit_length() + 7) // 8, byteorder="big")
        return base64.urlsafe_b64encode(b).decode("utf-8").rstrip("=")

    jwk = {
        "kty": "RSA",
        "use": "sig",
        "alg": "RS256",
        "kid": kid,
        "n": _b64(public_numbers.n),
        "e": _b64(public_numbers.e),
    }
    return private_key, json.dumps({"keys": [jwk]})


PRIVATE_KEY, JWKS_JSON = _gerar_par_rsa_e_jwks()


def _emitir_jwt(
    *,
    sub: UUID,
    actor_type: str,
    iss: str,
    aud: str = "oficina-backend-homolog",
    perfil: str | None = None,
    kid: str = "test-kid-1",
) -> str:
    agora = datetime.now(UTC)
    payload = {
        "sub": str(sub),
        "actor_type": actor_type,
        "iss": iss,
        "aud": aud,
        "iat": int(agora.timestamp()),
        "exp": int((agora + timedelta(minutes=30)).timestamp()),
    }
    if perfil:
        payload["perfil"] = perfil
    return jwt.encode(
        payload,
        PRIVATE_KEY,
        algorithm="RS256",
        headers={"kid": kid, "alg": "RS256"},
    )


@pytest.fixture
def app_client(tmp_path):
    db_file = tmp_path / "billing.sqlite"
    cfg = Configuracoes(
        database_url=f"sqlite:///{db_file}",
        jwks_public_keys_json=JWKS_JSON,
        mp_webhook_secret="segredo-webhook-mp",
        dd_trace_enabled=False,
    )
    app = criar_app(cfg)
    Base.metadata.create_all(bind=app.state.engine)
    return app, TestClient(app)


def test_healthchecks_e_observabilidade(app_client):
    _app, client = app_client
    assert client.get("/health").status_code == 200
    assert client.get("/health/live").json()["status"] == "alive"
    ready = client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["database"] == "connected"

    token = correlacao_ctx.set(
        {"correlation_id": "corr-1", "saga_id": "saga-1", "os_id": "os-1"}
    )
    try:
        logger = configurar_observabilidade(
            service="oficina-billing",
            env="homolog",
            version="0.1.0",
            trace_enabled=False,
        )
        logger.info("Teste log correlacionado")
    finally:
        correlacao_ctx.reset(token)


def test_orcamento_decisao_posse_e_pagamentos_api(app_client):
    app, client = app_client
    cliente_dono = uuid4()
    outro_cliente = uuid4()
    os_id = uuid4()
    saga_id = uuid4()

    with app.state.session_factory() as db:
        repo = OrcamentoRepositorioSql(db)
        orc = criar_orcamento_congelado(
            os_id=os_id,
            saga_id=saga_id,
            cliente_id=cliente_dono,
            rodada=1,
            versao=1,
            itens=[
                ItemOrcamento(
                    id=uuid4(),
                    tipo=TipoItemOrcamento.SERVICO,
                    referencia_id=uuid4(),
                    descricao="Alinhamento",
                    quantidade=1,
                    preco_unitario_centavos=12000,
                ),
                ItemOrcamento(
                    id=uuid4(),
                    tipo=TipoItemOrcamento.PECA,
                    referencia_id=uuid4(),
                    descricao="Pastilha",
                    quantidade=2,
                    preco_unitario_centavos=9000,
                ),
            ],
        )
        repo.salvar(orc)
        pag_id = uuid4()
        db.add(
            PagamentoModel(
                id=str(pag_id),
                orcamento_id=str(orc.id),
                os_id=str(os_id),
                saga_id=str(saga_id),
                cliente_id=str(cliente_dono),
                rodada=1,
                provider_order_id="ORD_01",
                qr_data="000201QR",
                status="PENDENTE",
                total_centavos=30000,
                moeda="BRL",
            )
        )
        db.commit()

    token_dono = _emitir_jwt(
        sub=cliente_dono, actor_type="CLIENTE", iss="oficina-auth-homolog"
    )
    token_outro = _emitir_jwt(
        sub=outro_cliente, actor_type="CLIENTE", iss="oficina-auth-homolog"
    )

    # Outro cliente não pode ver nem aprovar (403)
    r_forbidden = client.get(
        f"/v1/orcamentos/{orc.id}",
        headers={"Authorization": f"Bearer {token_outro}"},
    )
    assert r_forbidden.status_code == 403

    r_dec_forbidden = client.post(
        f"/v1/orcamentos/{orc.id}/decisao",
        json={"aprovado": True, "versao": 1},
        headers={"Authorization": f"Bearer {token_outro}"},
    )
    assert r_dec_forbidden.status_code == 403

    # Cliente dono consulta e aprova (202 Accepted)
    r_get = client.get(
        f"/v1/orcamentos/{orc.id}",
        headers={"Authorization": f"Bearer {token_dono}"},
    )
    assert r_get.status_code == 200
    assert r_get.json()["total_centavos"] == 30000

    # Versão errada retorna 409
    r_ver_err = client.post(
        f"/v1/orcamentos/{orc.id}/decisao",
        json={"aprovado": True, "versao": 2},
        headers={"Authorization": f"Bearer {token_dono}"},
    )
    assert r_ver_err.status_code == 409

    r_aprov = client.post(
        f"/v1/orcamentos/{orc.id}/decisao",
        json={"aprovado": True, "versao": 1},
        headers={"Authorization": f"Bearer {token_dono}"},
    )
    assert r_aprov.status_code == 202
    assert r_aprov.json()["status"] == StatusOrcamento.APROVADO.value

    # Pagamento consultado pelo dono vs outro cliente
    r_pag = client.get(
        f"/v1/pagamentos/{pag_id}",
        headers={"Authorization": f"Bearer {token_dono}"},
    )
    assert r_pag.status_code == 200
    assert r_pag.json()["qr_data"] == "000201QR"

    r_pag_outro = client.get(
        f"/v1/pagamentos/{pag_id}",
        headers={"Authorization": f"Bearer {token_outro}"},
    )
    assert r_pag_outro.status_code == 403


def test_webhook_mercadopago_assinatura_e_idempotencia(app_client):
    _, client = app_client
    assinatura = gerar_assinatura_webhook_mp(
        data_id="ORD_01",
        x_request_id="req-mp-100",
        ts="1763305200",
        webhook_secret="segredo-webhook-mp",
    )
    body = {"action": "order.processed", "data": {"id": "ORD_01"}}

    # Sem assinatura falha com 401
    r_unauth = client.post("/webhooks/mercadopago", json=body)
    assert r_unauth.status_code == 401

    # Com assinatura válida persiste e responde 200 (idempotente)
    r_ok1 = client.post(
        "/webhooks/mercadopago",
        json=body,
        headers={"x-signature": assinatura, "x-request-id": "req-mp-100"},
    )
    r_ok2 = client.post(
        "/webhooks/mercadopago",
        json=body,
        headers={"x-signature": assinatura, "x-request-id": "req-mp-100"},
    )
    assert r_ok1.status_code == 200
    assert r_ok2.status_code == 200


def test_validacao_jwt_rejeita_emissor_audience_ou_perfil_invalidos(app_client):
    _, client = app_client
    orc_id = uuid4()

    # Audience errada
    tok_aud = _emitir_jwt(
        sub=uuid4(),
        actor_type="CLIENTE",
        iss="oficina-auth-homolog",
        aud="oficina-backend-production",
    )
    assert (
        client.get(
            f"/v1/orcamentos/{orc_id}",
            headers={"Authorization": f"Bearer {tok_aud}"},
        ).status_code
        == 401
    )

    # Funcionário com emissor de cliente
    tok_iss_cruzado = _emitir_jwt(
        sub=uuid4(),
        actor_type="FUNCIONARIO",
        iss="oficina-auth-homolog",
        perfil="ADMIN",
    )
    assert (
        client.get(
            f"/v1/orcamentos/{orc_id}",
            headers={"Authorization": f"Bearer {tok_iss_cruzado}"},
        ).status_code
        == 401
    )


def test_contrato_barreira_cancelamento_antes_de_estorno_ou_cancelamento():
    fixtures = json.loads(
        open("docs/contratos/fixtures/fluxo-feliz-e-compensacao-v1.json").read()
    )
    # Comandos de compensação de Billing da fixture devem passar
    cmd_cancel_orc = fixtures["compensacao_falha_fila_apos_pagamento"][4]
    cmd_estorno = fixtures["compensacao_falha_fila_apos_pagamento"][5]

    assert (
        validar_comando_billing_v1(cmd_cancel_orc).event_type == "CancelarOrcamento.v1"
    )
    assert validar_comando_billing_v1(cmd_estorno).event_type == "EstornarPagamento.v1"

    # Sem execucao_cancelada_event_id deve ser rejeitado
    sem_barreira = {
        **cmd_estorno,
        "payload": {
            **cmd_estorno["payload"],
            "execucao_cancelada_event_id": "",
        },
    }
    with pytest.raises(ValueError, match="exige execucao_cancelada_event_id"):
        validar_comando_billing_v1(sem_barreira)


def test_adaptador_mercadopago_orders_client_e_fake():
    assert centavos_para_decimal_str(15000) == "150.00"
    assert decimal_str_para_centavos("150.00") == 15000
    assert mapear_status_order_mp("processed") == StatusPagamentoProvedor.CONFIRMADO

    fake = FakeMercadoPagoOrdersProvider(webhook_secret="segredo")
    ordem = fake.criar_cobranca(
        pagamento_id="pag-1",
        total_centavos=15000,
        moeda="BRL",
        descricao="OS 1",
        idempotency_key="k1",
    )
    headers, _ = fake.simular_pagamento_aprovado(ordem.provider_order_id)
    assert "x-signature" in headers
    consultada = fake.consultar(ordem.provider_order_id)
    validar_confirmacao_cobranca(
        consultada, pagamento_id_esperado="pag-1", total_centavos_esperado=15000
    )
    with pytest.raises(ErroDivergenciaConfirmacaoError):
        validar_confirmacao_cobranca(
            consultada, pagamento_id_esperado="outro", total_centavos_esperado=15000
        )
    estornada = fake.estornar(ordem.provider_order_id, idempotency_key="k2")
    assert estornada.status == StatusPagamentoProvedor.ESTORNADO

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/orders":
            return httpx.Response(
                201,
                json={
                    "id": "ORD1",
                    "external_reference": "pag-1",
                    "status": "created",
                    "total_amount": "150.00",
                    "currency": "BRL",
                    "type_response": {"qr_data": "QR1"},
                },
            )
        if request.url.path == "/v1/orders/ORD1/cancel":
            return httpx.Response(
                200,
                json={
                    "id": "ORD1",
                    "external_reference": "pag-1",
                    "status": "canceled",
                    "total_amount": "150.00",
                    "currency": "BRL",
                },
            )
        if request.url.path == "/v1/orders/ORD1/refund":
            return httpx.Response(
                201,
                json={
                    "id": "ORD1",
                    "external_reference": "pag-1",
                    "status": "refunded",
                    "total_amount": "150.00",
                    "currency": "BRL",
                },
            )
        if request.url.path == "/v1/orders/ERR500":
            return httpx.Response(500, text="err")
        return httpx.Response(404, text="not found")

    with httpx.Client(transport=httpx.MockTransport(handler)) as hc:
        cli = MercadoPagoOrdersClient(
            access_token="tok", external_pos_id="POS", http_client=hc
        )
        assert (
            cli.criar_cobranca(
                pagamento_id="pag-1",
                total_centavos=15000,
                moeda="BRL",
                descricao="d",
                idempotency_key="i1",
            ).qr_data
            == "QR1"
        )
        assert (
            cli.cancelar("ORD1", idempotency_key="i2").status
            == StatusPagamentoProvedor.CANCELADO
        )
        assert (
            cli.estornar("ORD1", idempotency_key="i3").status
            == StatusPagamentoProvedor.ESTORNADO
        )
        with pytest.raises(ErroComunicacaoProvedorError):
            cli.consultar("ERR500")
        with pytest.raises(ErroOperacaoProvedorError):
            cli.consultar("NAO_EXISTE")
