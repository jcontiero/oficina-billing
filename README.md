# Oficina Billing Service (`oficina-billing`)

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=jcontiero_oficina-billing&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=jcontiero_oficina-billing)

Microsserviço de Faturamento (Orçamentos versionados e Pagamentos via QR Dinâmico na Orders API do Mercado Pago) da Fase 4 do Tech Challenge.

## Responsabilidades
- Geração de orçamentos com preços congelados em centavos (`total_centavos`, `moeda="BRL"`).
- Registro de decisão do cliente (`APROVADO` / `RECUSADO`) com verificação de posse (`cliente_id`) e versão.
- Integração com **Mercado Pago Orders API v1 (`/v1/orders`)**: criação de QR dinâmico, consulta, cancelamento (`/cancel`) e estorno integral (`/refund`), além de recepção de webhook assinado (`x-signature`).
- Persistência isolada em PostgreSQL (`oficina_billing_{env}`) sem acesso nem chaves estrangeiras ao banco do OS Service.
- Exigência da barreira `execucao_cancelada_event_id` (`CancelarExecucao.v1` -> `ExecucaoCancelada.v1`) antes de processar comandos de compensação (`CancelarOrcamento.v1`, `CancelarCobranca.v1` e `EstornarPagamento.v1`).

## Execução Local e Testes
```bash
uv sync --all-extras
uv run ruff check src tests
uv run black --check src tests
uv run pytest tests/unit tests/integration tests/contract --cov=src --cov-fail-under=80
```
