from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
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
    FakeMercadoPagoOrdersProvider,
)
from src.shared.banco import Base
from src.shared.seguranca import ActorType, Principal, get_principal_atual


def _setup_billing(tmp_path: Path):
    db_url = f"sqlite:///{tmp_path / 'p6_billing.db'}"
    eng = create_engine(db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(eng)
    return db_url, sessionmaker(autocommit=False, autoflush=False, bind=eng)


def _cmd(event_type: str, *, saga_id, os_id, payload: dict) -> dict:
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


def test_reconciliar_exige_autenticacao_e_autorizacao_funcionario(
    tmp_path: Path,
) -> None:
    db_url, _ = _setup_billing(tmp_path)
    cfg = Configuracoes(
        database_url=db_url,
        environment="homolog",
        dd_trace_enabled=False,
        mp_provider_mode="fake",
        mp_webhook_secret="sec-p6",
    )
    app = criar_app(cfg)
    client = TestClient(app)

    # Sem token -> 401
    assert client.post("/v1/pagamentos/reconciliar").status_code == 401

    # CLIENTE -> 403
    app.dependency_overrides[get_principal_atual] = lambda: Principal(
        id=uuid4(), actor_type=ActorType.CLIENTE, perfil="CLIENTE"
    )
    assert client.post("/v1/pagamentos/reconciliar").status_code == 403

    # FUNCIONARIO -> 200
    app.dependency_overrides[get_principal_atual] = lambda: Principal(
        id=uuid4(), actor_type=ActorType.FUNCIONARIO, perfil="ADMIN"
    )
    r_ok = client.post("/v1/pagamentos/reconciliar")
    assert r_ok.status_code == 200
    assert r_ok.json() == {"reconciliadas": 0}


def test_falhas_repetidas_estorno_e_recuperacao_apos_reinicio_worker(
    tmp_path: Path,
) -> None:
    _, session_factory = _setup_billing(tmp_path)
    state_file = str(tmp_path / "mp_state.json")
    provedor_1 = FakeMercadoPagoOrdersProvider(
        webhook_secret="sec-p6", state_file=state_file
    )

    saga_id = uuid4()
    os_id = uuid4()
    with session_factory() as db:
        BillingConsumidor(db, provedor_1).processar_comando(
            _cmd(
                "CriarCobranca.v1",
                saga_id=saga_id,
                os_id=os_id,
                payload={
                    "rodada": 1,
                    "orcamento_id": str(uuid4()),
                    "orcamento_versao": 1,
                    "cliente_id": str(uuid4()),
                    "total_centavos": 22000,
                    "moeda": "BRL",
                },
            )
        )
        pag = db.query(PagamentoModel).one()
        pag_id = pag.id
        order_id = pag.provider_order_id
        provedor_1.simular_pagamento_aprovado(order_id)
        pag.status = "CONFIRMADO"
        db.commit()

    # Falha 1 e Falha 2 no estorno (timeout transitório) -> mantém mesma idempotency_key
    barreira_id = str(uuid4())
    for tentativa in range(2):
        provedor_1.configurar_falha(
            ErroComunicacaoProvedorError(f"timeout estorno #{tentativa + 1}")
        )
        with session_factory() as db:
            BillingConsumidor(db, provedor_1).processar_comando(
                _cmd(
                    "EstornarPagamento.v1",
                    saga_id=saga_id,
                    os_id=os_id,
                    payload={
                        "rodada": 1,
                        "compensacao_id": str(uuid4()),
                        "motivo": "FALHA_ENFILEIRAMENTO",
                        "pagamento_id": pag_id,
                        "total_centavos": 22000,
                        "moeda": "BRL",
                        "execucao_cancelada_event_id": barreira_id,
                    },
                )
            )
            ops_ref = (
                db.query(ProviderOperationModel)
                .filter(ProviderOperationModel.tipo == "REFUND")
                .all()
            )
            assert len(ops_ref) == 1
            assert ops_ref[0].idempotency_key == f"refund-{pag_id}"
            assert ops_ref[0].estado == "DESCONHECIDO"

    # Reinício do worker: nova instância do FakeMercadoPagoOrdersProvider lendo state_file
    provedor_reiniciado = FakeMercadoPagoOrdersProvider(
        webhook_secret="sec-p6", state_file=state_file
    )
    # Falha mais uma vez durante reconciliação sem quebrar o worker
    provedor_reiniciado.configurar_falha(
        ErroComunicacaoProvedorError("timeout durante reconciliacao")
    )
    with session_factory() as db:
        assert (
            BillingConsumidor(db, provedor_reiniciado).reconciliar_operacoes_pendentes()
            == 0
        )

    # Na próxima rodada após o provedor normalizar, reconcilia com a mesma idempotency_key
    with session_factory() as db:
        assert (
            BillingConsumidor(db, provedor_reiniciado).reconciliar_operacoes_pendentes()
            == 1
        )
        ops_ref = (
            db.query(ProviderOperationModel)
            .filter(ProviderOperationModel.tipo == "REFUND")
            .all()
        )
        assert len(ops_ref) == 1
        assert ops_ref[0].idempotency_key == f"refund-{pag_id}"
        assert ops_ref[0].estado == "SUCESSO"
        pag = db.query(PagamentoModel).one()
        assert pag.status == "ESTORNADO"
        evts = [e.tipo_evento for e in db.query(OutboxEventoModel).all()]
        assert evts.count("PagamentoEstornoFalhou.v1") == 2
        assert evts.count("PagamentoEstornado.v1") == 1
