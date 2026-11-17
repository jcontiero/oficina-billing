from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine

from src.config import Configuracoes
from src.pagamentos.infraestrutura.fake_persistente import (
    EstadoProvedorFakeModel,
    ProvedorFakePersistente,
)
from src.pagamentos.infraestrutura.mercadopago import StatusPagamentoProvedor
from src.worker import criar_provedor_pagamento


def test_fake_compartilha_estado_e_idempotencia_apos_reinicio(tmp_path):
    url = f"sqlite:///{tmp_path / 'fake.db'}"
    EstadoProvedorFakeModel.__table__.create(create_engine(url))
    config = Configuracoes(database_url=url, mp_fake_shared_database=True)
    worker = criar_provedor_pagamento(config)
    api = criar_provedor_pagamento(config)
    args = dict(
        pagamento_id="pagamento-1",
        total_centavos=1000,
        moeda="BRL",
        descricao="Reparo",
        idempotency_key="criar-1",
    )
    with ThreadPoolExecutor(max_workers=4) as pool:
        ordens = list(pool.map(lambda _: worker.criar_cobranca(**args), range(8)))
    ordem = ordens[0]
    assert len({o.provider_order_id for o in ordens}) == 1
    assert (
        api.buscar_por_external_reference("pagamento-1").provider_order_id
        == ordem.provider_order_id
    )
    api.simular_pagamento_aprovado(ordem.provider_order_id)
    reiniciado = ProvedorFakePersistente(url, config.mp_webhook_secret)
    assert (
        reiniciado.consultar(ordem.provider_order_id).status
        == StatusPagamentoProvedor.CONFIRMADO
    )
    reiniciado.estornar(ordem.provider_order_id, idempotency_key="estornar-1")
    assert (
        worker.estornar(ordem.provider_order_id, idempotency_key="estornar-1").status
        == StatusPagamentoProvedor.ESTORNADO
    )
    with pytest.raises(AttributeError):
        api.metodo_inexistente()


def test_fake_persiste_cancelamento_e_recusa(tmp_path):
    url = f"sqlite:///{tmp_path / 'fake.db'}"
    EstadoProvedorFakeModel.__table__.create(create_engine(url))
    api = ProvedorFakePersistente(url, "secret")

    def criar(chave):
        return api.criar_cobranca(
            pagamento_id=chave,
            total_centavos=1,
            moeda="BRL",
            descricao="Teste",
            idempotency_key=chave,
        )

    ordem = criar("cancelar")
    api.cancelar(ordem.provider_order_id, idempotency_key="cancelar-1")
    outra = ProvedorFakePersistente(url, "secret")
    assert (
        outra.consultar(ordem.provider_order_id).status
        == StatusPagamentoProvedor.CANCELADO
    )
    recusada = criar("recusar")
    api.simular_pagamento_recusado(recusada.provider_order_id)
    assert (
        outra.consultar(recusada.provider_order_id).status
        == StatusPagamentoProvedor.RECUSADO
    )
    assert outra.buscar_por_external_reference("inexistente") is None


@pytest.mark.parametrize("acao", ["pagar", "recusar"])
def test_simulacao_financeira_exige_admin(tmp_path, acao):
    from uuid import uuid4
    from fastapi.testclient import TestClient
    from src.main import criar_app
    from src.shared.seguranca import ActorType, Principal, get_principal_atual

    app = criar_app(Configuracoes(database_url=f"sqlite:///{tmp_path / 'auth.db'}"))
    provider = app.state.provedor_pagamento
    ordem = provider.criar_cobranca(
        pagamento_id="p1",
        total_centavos=100,
        moeda="BRL",
        descricao="Teste",
        idempotency_key="p1",
    )
    client = TestClient(app)
    url = f"/internal/fake-mp/{ordem.provider_order_id}/{acao}"
    assert client.post(url).status_code == 401
    app.dependency_overrides[get_principal_atual] = lambda: Principal(
        id=uuid4(), actor_type=ActorType.CLIENTE, perfil="CLIENTE"
    )
    assert client.post(url).status_code == 403
    app.dependency_overrides[get_principal_atual] = lambda: Principal(
        id=uuid4(), actor_type=ActorType.FUNCIONARIO, perfil="ADMIN"
    )
    assert client.post(url).status_code == 200
