# Roteiro de Aceite Externo — Mercado Pago (Orders API v1 + QR Dinâmico + Webhooks)

> Este roteiro destina-se exclusivamente à validação externa futura no ambiente de teste do Mercado Pago quando `MP_ACCESS_TOKEN`, `MP_WEBHOOK_SECRET` real e uma URL HTTPS pública para entrega de webhooks estiverem disponíveis.
> Os testes unitários com `MockTransport` e a stack local com `FakeMercadoPagoOrdersProvider` validam o comportamento interno do adaptador e da Saga, mas **não** comprovam que o provedor externo aceita o contrato em produção/sandbox.

## Pré-requisitos

1. Credenciais de aplicação de teste no Mercado Pago:
   - `MP_PROVIDER_MODE=mercadopago`
   - `MP_ACCESS_TOKEN=<token_de_teste>`
   - `MP_EXTERNAL_POS_ID=<pos_id_cadastrado>`
   - `MP_WEBHOOK_SECRET=<secret_webhook_painel_mp>`
2. URL HTTPS pública apontando para `POST /webhooks/mercadopago` do serviço `oficina-billing`.

## Endpoints Oficiais Conferidos (Orders API v1)

| Etapa | Método e Endpoint | Headers Obrigatórios | Resultado Esperado |
|---|---|---|---|
| 1. Criação de Ordem / QR | `POST https://api.mercadopago.com/v1/orders` | `Authorization: Bearer ...`, `X-Idempotency-Key: create-{saga_id}-{rodada}` | `201 Created`, `id` da order, `status=created`, `type_response.qr_data` |
| 2. Consulta por ID | `GET https://api.mercadopago.com/v1/orders/{order_id}` | `Authorization: Bearer ...` | `200 OK`, status atualizado (`created`, `processed`, `failed`, `canceled`, `refunded`) |
| 3. Busca / Reconciliação | `GET https://api.mercadopago.com/v1/orders?begin_date=...&end_date=...` (ou filtro suportado pela conta) | `Authorization: Bearer ...` | `200 OK`, localização da order criada em caso de timeout após criação |
| 4. Cancelamento (não paga) | `POST https://api.mercadopago.com/v1/orders/{order_id}/cancel` | `Authorization: Bearer ...`, `X-Idempotency-Key: cancel-{pagamento_id}` | `200 OK` / `202 Accepted`, `status=canceled` |
| 5. Estorno total (paga) | `POST https://api.mercadopago.com/v1/orders/{order_id}/refund` | `Authorization: Bearer ...`, `X-Idempotency-Key: refund-{pagamento_id}` | `201 Created` / `200 OK`, `status=refunded` |

## Roteiro de Execução Manual/Assistida

1. **Criação de QR:** abrir OS, concluir diagnóstico, aprovar orçamento e confirmar em `GET /v1/pagamentos/por-os/{os_id}` que `provider_order_id` e `qr_data` foram persistidos com `status=PENDENTE`.
2. **Pagamento de teste:** efetuar o pagamento usando conta compradora de teste no QR gerado.
3. **Webhook assinado e consulta:** confirmar recebimento de `POST /webhooks/mercadopago` com cabeçalhos `x-signature: ts=...,v1=...` e `x-request-id`. Verificar nos logs estruturados (`dd.service=oficina-billing`) a validação HMAC-SHA256 e a chamada `GET /v1/orders/{order_id}` antes da emissão de `PagamentoConfirmado.v1`.
4. **Cancelamento e Estorno:**
   - Em uma OS não paga, acionar cancelamento e confirmar `POST /v1/orders/{order_id}/cancel`.
   - Em uma OS paga antes do início do reparo, acionar cancelamento e confirmar `POST /v1/orders/{order_id}/refund` somente após `ExecucaoCancelada.v1`.
5. **Reconciliação:** simular timeout na criação ou no estorno, validar registro `DESCONHECIDO` em `provider_operations` e acionar `POST /v1/pagamentos/reconciliar` (com token `FUNCIONARIO`), confirmando convergência sem cobrança ou estorno duplicado.
