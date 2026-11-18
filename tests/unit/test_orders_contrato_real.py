import hashlib
import hmac

import httpx

from src.pagamentos.infraestrutura.mercadopago import (
    MercadoPagoOrdersClient,
    validar_assinatura_webhook_mp,
)


def test_assinatura_oficial_normaliza_order_alfanumerica():
    manifesto = b"id:ordtst123;request-id:request-test;ts:1700000000000;"
    digest = hmac.new(b"secret-test", manifesto, hashlib.sha256).hexdigest()
    assert validar_assinatura_webhook_mp(
        x_signature=f"ts=1700000000000,v1={digest}",
        x_request_id="request-test",
        data_id="ORDTST123",
        webhook_secret="secret-test",
    )


def test_busca_orders_inclui_datas_e_codifica_referencia():
    def handler(request):
        assert request.url.params["external_reference"] == "pagamento+teste"
        assert request.url.params["begin_date"] < request.url.params["end_date"]
        return httpx.Response(200, json={"elements": []})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        provider = MercadoPagoOrdersClient(
            access_token="token-test", external_pos_id="POS", http_client=client
        )
        assert provider.buscar_por_external_reference("pagamento+teste") is None
