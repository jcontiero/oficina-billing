from src.pagamentos.infraestrutura import fake_persistente  # noqa: F401
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.config import Configuracoes
from src.mensageria import outbox_inbox as _outbox_inbox  # noqa: F401
from src.orcamentos.apresentacao.rotas import router as billing_router
from src.orcamentos.infraestrutura import modelos as _modelos  # noqa: F401
from src.shared.banco import Base
from src.shared.observabilidade import configurar_observabilidade
from src.shared.seguranca import ValidadorJwtRs256
from src.worker import criar_provedor_pagamento


def criar_app(configuracoes: Configuracoes | None = None) -> FastAPI:
    config = configuracoes or Configuracoes()
    configurar_observabilidade(
        service=config.dd_service,
        env=config.dd_env,
        version=config.dd_version,
        trace_enabled=config.dd_trace_enabled,
    )

    app = FastAPI(
        title="Oficina Billing Service API",
        description="Microsserviço de Orçamentos e Pagamentos QR Dinâmico Mercado Pago",
        version=config.dd_version,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    engine = create_engine(config.database_url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    if config.environment in {"homolog", "local", "test"}:
        try:
            Base.metadata.create_all(bind=engine)
        except Exception:
            pass

    app.state.config = config
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.validador_jwt = ValidadorJwtRs256(config)
    app.state.provedor_pagamento = criar_provedor_pagamento(config)

    @app.get("/health", tags=["health"])
    def health_check() -> dict[str, str]:
        return {"status": "ok", "service": "oficina-billing"}

    @app.get("/health/live", tags=["health"])
    def liveness_check() -> dict[str, str]:
        return {"status": "alive", "service": "oficina-billing"}

    @app.get("/health/ready", tags=["health"])
    def readiness_check() -> dict[str, str]:
        try:
            with app.state.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {
                "status": "ready",
                "service": "oficina-billing",
                "database": "connected",
            }
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Database not ready: {exc}",
            ) from exc

    app.include_router(billing_router)
    return app


app = criar_app()
