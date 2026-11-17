from pydantic_settings import BaseSettings, SettingsConfigDict


class Configuracoes(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite:///./billing_test.db"
    environment: str = "homolog"
    gcp_project_id: str = "fiap-project-514922"

    dd_service: str = "oficina-billing"
    dd_env: str = "homolog"
    dd_version: str = "0.1.0"
    dd_trace_enabled: bool = False

    jwt_audience: str = "oficina-backend-homolog"
    jwt_client_issuer: str = "oficina-auth-homolog"
    jwt_admin_issuer: str = "oficina-admin-homolog"
    client_jwks_url: str = "http://localhost:8080/.well-known/jwks.json"
    admin_jwks_url: str = "http://localhost:8000/.well-known/admin-jwks.json"
    jwks_public_keys_json: str = ""

    mp_access_token: str = "TEST-ACCESS-TOKEN"
    mp_external_pos_id: str = "POS001"
    mp_webhook_secret: str = "test-mp-webhook-secret"
    mp_base_url: str = "https://api.mercadopago.com"
