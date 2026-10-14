# Especificação de Mensagens — Contrato de Comunicação

Este documento é o **contrato de comunicação** entre os microsserviços do Freela Marketplace.
Todo evento publicado no Kafka segue o **envelope padronizado** descrito abaixo. Consumidores devem
depender apenas deste contrato.

## Envelope padronizado

```json
{
  "eventId":       "UUID  — id único do evento (idempotência)",
  "eventType":     "String — tipo do evento",
  "contratoId":    "UUID  — agregado (também é a chave de publicação)",
  "occurredAt":    "Instant ISO-8601 — quando o fato ocorreu",
  "correlationId": "String — correlação da operação (rastreamento)",
  "data":          { "...": "payload específico do evento" }
}
```

**Serialização:** JSON (String) · **Key da mensagem:** `contratoId` · **Headers:** `eventId`,
`eventType`, `X-Correlation-Id`.

---

## Evento: ContratoCriado

| Atributo | Valor |
|---|---|
| Nome | `ContratoCriado` |
| Tópico | `contratos.contrato-criado` |
| Produtor | contrato-service |
| Consumidores | notificacao-service, auditoria-service |
| Chave de publicação | `contratoId` |
| Campos obrigatórios (`data`) | `contratoId`, `clienteId`, `freelancerId`, `titulo`, `valor` |

```json
{
  "eventId": "9bff5d43-6e09-4a63-bb33-7f617c52ab91",
  "eventType": "ContratoCriado",
  "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
  "occurredAt": "2026-09-23T02:07:53.9Z",
  "correlationId": "demo-001",
  "data": {
    "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construção de API de pagamentos",
    "valor": 3500.00
  }
}
```

---

## Evento: EntregaRegistrada

| Atributo | Valor |
|---|---|
| Nome | `EntregaRegistrada` |
| Tópico | `contratos.entrega-registrada` |
| Produtor | contrato-service |
| Consumidores | notificacao-service, auditoria-service |
| Chave de publicação | `contratoId` |
| Campos obrigatórios (`data`) | `contratoId`, `clienteId`, `freelancerId` |

```json
{
  "eventId": "3d182bff-ffaf-446e-ad48-b25441cb2b25",
  "eventType": "EntregaRegistrada",
  "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
  "occurredAt": "2026-09-23T02:07:56.2Z",
  "correlationId": "demo-001",
  "data": {
    "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222"
  }
}
```

---

## Evento: ContratoConcluido

| Atributo | Valor |
|---|---|
| Nome | `ContratoConcluido` |
| Tópico | `contratos.contrato-concluido` |
| Produtor | contrato-service |
| Consumidores | notificacao-service, **reputacao-service**, auditoria-service |
| Chave de publicação | `contratoId` |
| Campos obrigatórios (`data`) | `contratoId`, `clienteId`, `freelancerId`, `valor` |

> `valor` é obrigatório porque o reputacao-service o agrega no total do freelancer.

```json
{
  "eventId": "9d5004aa-83d3-4c31-9b75-6880992e93ed",
  "eventType": "ContratoConcluido",
  "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
  "occurredAt": "2026-09-23T02:07:56.2Z",
  "correlationId": "demo-001",
  "data": {
    "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "valor": 3500.00
  }
}
```

---

## Evento: ContratoCancelado

| Atributo | Valor |
|---|---|
| Nome | `ContratoCancelado` |
| Tópico | `contratos.contrato-cancelado` |
| Produtor | contrato-service |
| Consumidores | notificacao-service, auditoria-service |
| Chave de publicação | `contratoId` |
| Campos obrigatórios (`data`) | `contratoId`, `clienteId`, `freelancerId`, `motivo` |

```json
{
  "eventId": "b1a2c3d4-0000-0000-0000-000000000000",
  "eventType": "ContratoCancelado",
  "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
  "occurredAt": "2026-09-23T02:08:10.0Z",
  "correlationId": "demo-001",
  "data": {
    "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "motivo": "cliente desistiu"
  }
}
```

---

## Regras transversais

- **Ordenação:** eventos do mesmo `contratoId` são publicados na mesma partição (key=contratoId) e
  processados em ordem.
- **Idempotência:** consumidores descartam mensagens cujo `eventId` já foi processado
  (tabela `processed_events`).
- **Falhas:** após N tentativas, a mensagem vai para `<tópico>.DLT`.
- **Rastreamento:** `correlationId` acompanha a operação em logs e mensagens; o trace (Zipkin) é
  propagado via headers Kafka.
