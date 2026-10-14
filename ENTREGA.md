# Entrega — Comunicação Assíncrona no Freela Marketplace

Este documento descreve a solução implementada sobre a aplicação base, evoluindo a
comunicação entre os microsserviços para um modelo **orientado a eventos** com Apache Kafka,
adicionando confiabilidade (Outbox transacional + DLQ), idempotência, ordenação por contrato,
logs centralizados e rastreamento distribuído.

> Toda a solução roda **localmente** via Docker Compose + Maven. Nada de nuvem.

---

## 1. Visão geral da arquitetura

```
Cliente
  │ HTTP (X-Correlation-Id)
  ▼
API Gateway (8080)  ──registra/descobre──►  Eureka (8761)
  │ roteia
  ▼
contrato-service (8081)
  │  1. persiste o Contrato   ┐
  │  2. grava evento no Outbox ┘  (MESMA transação)
  │  3. OutboxRelay (@Scheduled) publica no Kafka
  ▼
Apache Kafka (9092, KRaft)  — key = contratoId
  ├────────────► notificacao-service (8082) → notificacao_db
  ├────────────► reputacao-service   (8083) → reputacao_db   (só ContratoConcluido)
  └────────────► auditoria-service   (8084) → auditoria_db

Observabilidade:
  • Logs   → Loki (3100) → Grafana (3000)
  • Traces → Zipkin (9411)
  • Kafka UI (8090)
```

### Serviços participantes

| Serviço | Porta | Banco | Papel |
|---|---|---|---|
| api-gateway | 8080 | — | Entrada HTTP, gera/propaga `X-Correlation-Id` |
| eureka-server | 8761 | — | Service discovery |
| contrato-service | 8081 | contrato_db | **Produtor** dos eventos (Outbox) |
| notificacao-service | 8082 | notificacao_db | Consumidor — registra notificações |
| reputacao-service | 8083 | reputacao_db | Consumidor — atualiza reputação (ContratoConcluido) |
| auditoria-service | 8084 | auditoria_db | Consumidor — registra todos os eventos |

---

## 2. Tópicos Kafka

Todos os tópicos são criados com **3 partições** (habilita concorrência) e réplica 1 (ambiente local
de um broker). Definidos em `contrato-service/.../infrastructure/kafka/KafkaTopicsConfig.java`.

| Tópico | Produtor | Consumidores |
|---|---|---|
| `contratos.contrato-criado` | contrato-service | notificacao, auditoria |
| `contratos.entrega-registrada` | contrato-service | notificacao, auditoria |
| `contratos.contrato-concluido` | contrato-service | notificacao, reputacao, auditoria |
| `contratos.contrato-cancelado` | contrato-service | notificacao, auditoria |
| `<tópico>.DLT` | (auto) | Dead Letter Topic por consumidor |

**Chave de particionamento:** `contratoId` em todos os tópicos. Garante que os eventos de um mesmo
contrato caem sempre na mesma partição → ordem preservada por contrato.

---

## 3. Especificação das mensagens (contrato de comunicação)

Todas as mensagens usam um **envelope padronizado** (JSON). Definido em
`contrato-service/.../infrastructure/outbox/EventEnvelope.java` e espelhado em cada consumidor.

### Envelope

| Campo | Tipo | Descrição |
|---|---|---|
| `eventId` | UUID | Identificador único do evento (**chave de idempotência**) |
| `eventType` | String | Tipo do evento |
| `contratoId` | UUID | Agregado relacionado (**também é a chave de publicação**) |
| `occurredAt` | Instant (ISO-8601) | Momento em que o fato ocorreu no domínio |
| `correlationId` | String | Correlação da operação (rastreamento ponta a ponta) |
| `data` | objeto | Payload específico do evento |

Headers Kafka anexados: `eventId`, `eventType`, `X-Correlation-Id`.

### Eventos

#### `ContratoCriado` — tópico `contratos.contrato-criado`
- **Produtor:** contrato-service · **Consumidores:** notificacao, auditoria
- **Chave:** `contratoId`
- **`data` obrigatório:** `contratoId`, `clienteId`, `freelancerId`, `titulo`, `valor`

```json
{
  "eventId": "9bff5d43-6e09-4a63-bb33-7f617c52ab91",
  "eventType": "ContratoCriado",
  "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
  "occurredAt": "2026-09-23T02:07:53.9Z",
  "correlationId": "smoke-teste-002",
  "data": {
    "eventId": "9bff5d43-6e09-4a63-bb33-7f617c52ab91",
    "occurredAt": "2026-09-23T02:07:53.9Z",
    "contratoId": "64ba368f-196e-4682-aeb7-59723976a579",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Construção de API de pagamentos",
    "valor": 3500.00
  }
}
```

#### `EntregaRegistrada` — tópico `contratos.entrega-registrada`
- **Produtor:** contrato-service · **Consumidores:** notificacao, auditoria
- **`data` obrigatório:** `contratoId`, `clienteId`, `freelancerId`

#### `ContratoConcluido` — tópico `contratos.contrato-concluido`
- **Produtor:** contrato-service · **Consumidores:** notificacao, **reputacao**, auditoria
- **`data` obrigatório:** `contratoId`, `clienteId`, `freelancerId`, `valor`
- O `valor` é necessário para o reputacao-service agregar o total.

#### `ContratoCancelado` — tópico `contratos.contrato-cancelado`
- **Produtor:** contrato-service · **Consumidores:** notificacao, auditoria
- **`data` obrigatório:** `contratoId`, `clienteId`, `freelancerId`, `motivo`

---

## 4. Publicação transacional (Transactional Outbox)

Problema: se o serviço persistisse o contrato e depois publicasse no Kafka, uma falha entre os dois
passos deixaria o sistema inconsistente (contrato salvo sem evento, ou evento sem contrato).

Solução implementada — **padrão Outbox**:

1. No mesmo `@Transactional` do caso de uso, o `contrato-service`:
   - persiste o `Contrato` (aggregate);
   - grava cada evento na tabela `outbox_events` (`OutboxRecorder`).
   - Como as duas escritas estão na **mesma transação do mesmo banco**, ou ambas ocorrem ou nenhuma.
2. Um relay assíncrono `OutboxRelay` (`@Scheduled`, a cada 2s):
   - lê os eventos com `status = PENDENTE` por ordem de criação;
   - publica no Kafka de forma **síncrona** (`send().get()`), com `key = contratoId`;
   - só marca como `PUBLICADO` após o `ack` do broker. Se falhar, permanece `PENDENTE` e é
     retentado no próximo ciclo (garante *at-least-once*).

O producer usa `acks=all` + `enable.idempotence=true` + `max.in.flight=1` (ordem e sem duplicação no
lado do broker).

Tabela `outbox_events`: `eventId (PK)`, `tipo`, `topico`, `chaveParticao`, `contratoId`,
`correlationId`, `payload`, `status`, `criadoEm`, `publicadoEm`, `tentativas`.

---

## 5. Estratégia de particionamento e ordenação

- **Particionamento por `contratoId`** (chave da mensagem). Eventos do mesmo contrato → mesma partição.
- Tópicos com **3 partições**; consumidores com `concurrency: 3` (3 threads).
- **Ordem garantida por contrato:** dentro de uma partição o Kafka é ordenado, e o relay publica os
  eventos de um contrato na ordem em que foram gravados no outbox.
- **Concorrência entre contratos:** contratos diferentes podem cair em partições diferentes e ser
  processados em paralelo, sem afetar a ordem intra-contrato.

Exemplo garantido não sair de ordem para o mesmo contrato:
`ContratoCriado → EntregaRegistrada → ContratoConcluido`.

---

## 6. Idempotência

- Cada evento tem um `eventId` único.
- Cada consumidor mantém uma tabela `processed_events` (`eventId` como PK).
- Antes de processar, o consumidor verifica `existsById(eventId)`:
  - se já existe → loga `*.evento.duplicado.ignorado` e **descarta** (nenhum efeito colateral);
  - senão → processa e grava o `eventId`, **na mesma transação**.

Resultado: reprocessar a mesma mensagem **não** duplica registros, não incrementa reputação de novo,
nem gera notificações repetidas.

---

## 7. Tratamento de falhas (retry + DLQ)

Configurado em `KafkaErrorHandlingConfig` de cada consumidor:

- `DefaultErrorHandler` com `FixedBackOff` → **3 tentativas** com 1s de intervalo.
- Esgotadas as tentativas, a mensagem vai para o **Dead Letter Topic** `<tópico>.DLT` (mesma partição),
  via `DeadLetterPublishingRecoverer`.
- A falha em uma mensagem **não compromete** as demais: o consumo do restante da partição prossegue.
- As mensagens no `.DLT` ficam disponíveis para diagnóstico e reprocessamento.

Parâmetros: `contrato.consumo.max-tentativas` e `contrato.consumo.intervalo-retry-ms`.

---

## 8. Logs centralizados

- Todos os serviços usam um `logback-spring.xml` com dois appenders: **console** e **Loki**
  (`loki-logback-appender`).
- Cada linha de log carrega, via MDC: `service`, `traceId`, `spanId`, `correlationId`, `contratoId`,
  `eventId`.
- O **Grafana** (com datasource Loki provisionado) permite pesquisar uma operação inteira entre todos
  os serviços por um campo comum. Ex. de query no Grafana/Explore:

  ```
  {app=~".+"} |= "smoke-trace-004"
  ```

  retorna as linhas de gateway, contrato e os 3 consumidores para aquele `correlationId`.

---

## 9. Rastreamento distribuído (Zipkin)

- Micrometer Tracing (bridge Brave) + exportação para **Zipkin**.
- `management.tracing.sampling.probability: 1.0` (captura tudo, para o ambiente de estudo).
- Observation habilitado no Kafka (`spring.kafka.template.observation-enabled` no produtor e
  `spring.kafka.listener.observation-enabled` nos consumidores) → o contexto de trace é propagado
  **através das mensagens Kafka**.
- No Zipkin é possível ver a participação dos serviços numa operação (produtor → consumidores).

**Observação importante sobre o desenho:** como a publicação é feita pelo `OutboxRelay` num thread
agendado (desacoplado da requisição HTTP original — que é o correto para o padrão Outbox), o trace da
requisição HTTP (gateway → contrato) e o trace da publicação/consumo (relay → Kafka → consumidores)
aparecem como **dois traces distintos** no Zipkin. O elo lógico ponta a ponta entre eles é o
**`correlationId`**, presente nos logs e no envelope da mensagem. O trace de consumo, esse sim, liga
os três consumidores numa mesma árvore.

---

## 10. Correlação das operações

`API Gateway → contrato-service → Kafka → consumidores`:

- O gateway gera um `X-Correlation-Id` (ou usa o recebido) e o propaga.
- O contrato-service coloca o `correlationId` no MDC (logs) e no envelope de cada evento.
- Os consumidores restauram o `correlationId` do envelope para o MDC ao processar.

Assim a mesma operação é correlacionável em logs e mensagens de todos os serviços.

---

## 11. Como executar (Windows + Docker Desktop)

### Pré-requisitos
- **Java 21** (JDK). Confirme com `java -version`.
- **Docker Desktop** aberto e rodando.
- **Maven Wrapper** já incluído (`mvnw.cmd`) — **não precisa instalar Maven**.

Use o **PowerShell** ou o **Prompt de Comando (CMD)**, sempre a partir da **raiz do projeto**.

### 11.1. Subir a infraestrutura

Com o Docker Desktop aberto:

```bat
cd infra
docker compose up -d
docker compose ps
```

Para encerrar depois: `docker compose down` (ou `docker compose down -v` para apagar os dados).

### 11.2. Subir as aplicações

Abra **6 terminais** (um por serviço), todos na **raiz do projeto**. Suba o Eureka primeiro e
aguarde ~15s antes dos demais.

```bat
mvnw.cmd -pl eureka-server spring-boot:run
mvnw.cmd -pl api-gateway spring-boot:run
mvnw.cmd -pl contrato-service spring-boot:run
mvnw.cmd -pl notificacao-service spring-boot:run
mvnw.cmd -pl reputacao-service spring-boot:run
mvnw.cmd -pl auditoria-service spring-boot:run
```

> Se a porta **8080** (gateway) já estiver ocupada, suba o gateway em outra porta:
> `mvnw.cmd -pl api-gateway spring-boot:run "-Dspring-boot.run.arguments=--server.port=8088"`
> (e troque 8080 por 8088 nos comandos da seção 12).

### Consoles
- Eureka: http://localhost:8761
- Kafka UI: http://localhost:8090
- Zipkin: http://localhost:9411
- Grafana (logs): http://localhost:3000  (datasource Loki já provisionado)

---

## 12. Exemplos de chamadas (geração de eventos)

O Windows 10/11 já inclui o `curl` (funciona no PowerShell e no CMD). Comandos em uma linha:

```bat
REM 1) Criar contrato (evento ContratoCriado)
curl -i -X POST http://localhost:8080/api/contratos -H "Content-Type: application/json" -H "X-Correlation-Id: demo-001" -d "{\"clienteId\":\"11111111-1111-1111-1111-111111111111\",\"freelancerId\":\"22222222-2222-2222-2222-222222222222\",\"titulo\":\"Construcao de API de pagamentos\",\"valor\":3500.00}"

REM copie o "id" retornado e use no lugar de <ID> abaixo
REM 2) Registrar entrega
curl -X POST http://localhost:8080/api/contratos/<ID>/entrega -H "X-Correlation-Id: demo-001"

REM 3) Concluir (dispara reputacao)
curl -X POST http://localhost:8080/api/contratos/<ID>/conclusao -H "X-Correlation-Id: demo-001"

REM (alternativa) Cancelar
curl -X POST http://localhost:8080/api/contratos/<ID>/cancelamento -H "Content-Type: application/json" -H "X-Correlation-Id: demo-001" -d "{\"motivo\":\"cliente desistiu\"}"

REM Consultas
curl http://localhost:8080/api/contratos/<ID>
curl http://localhost:8080/api/reputacoes
curl http://localhost:8080/api/auditoria
```

> **Nota:** os endpoints de consulta de auditoria/reputação/notificação vêm da aplicação base e
> serializam objetos vazios em JSON (as entidades originais não expõem getters). Os dados **são**
> persistidos — verifique via banco (seção 13) ou pelos logs/Grafana.

---

## 13. Evidências da execução (coletadas no smoke test)

Todos os fluxos abaixo foram executados e verificados.

### Fluxo HTTP + mudança de estado
```
POST /api/contratos          → status ATIVO
POST /{id}/entrega           → status ENTREGA_REGISTRADA
POST /{id}/conclusao         → status CONCLUIDO
```

### Evento publicado (Outbox) — `contrato_db`
```sql
SELECT tipo, status, tentativas FROM outbox_events ORDER BY criado_em;
--  ContratoCriado    | PUBLICADO | 1
--  EntregaRegistrada | PUBLICADO | 1
--  ContratoConcluido | PUBLICADO | 1
```

### Consumo + persistência
```sql
-- auditoria_db
SELECT event_type, aggregate_id, correlation_id FROM auditoria_eventos ORDER BY recebido_em;
--  ContratoCriado / EntregaRegistrada / ContratoConcluido  (correlation_id preenchido)

-- reputacao_db
SELECT freelancer_id, contratos_concluidos, valor_total FROM reputacoes;
--  22222222-... | 1 | 3500.00     (reagiu só ao ContratoConcluido)

-- notificacao_db
SELECT tipo, contrato_id, destinatario_id FROM notificacoes ORDER BY criada_em;
--  destinatário correto por tipo de evento
```

### Ordem por contrato (logs do auditoria-service)
```
auditoria.evento.processado ... eventType=ContratoCriado
auditoria.evento.processado ... eventType=EntregaRegistrada
auditoria.evento.processado ... eventType=ContratoConcluido
```

### Idempotência (mensagem duplicada)
Reenviando o mesmo `ContratoConcluido` (mesmo `eventId`) ao tópico, a reputação **não** muda e os
três consumidores logam:
```
reputacao.evento.duplicado.ignorado  eventId=8d1c8af3-... eventType=ContratoConcluido
auditoria.evento.duplicado.ignorado  eventId=8d1c8af3-...
notificacao.evento.duplicado.ignorado eventId=8d1c8af3-...
```

### Tracing (Zipkin)
```
GET http://localhost:9411/api/v2/services
→ ["api-gateway","auditoria-service","contrato-service","notificacao-service","reputacao-service"]
```
Existe um trace ligando `contrato-service → auditoria + notificacao + reputacao` (fluxo Kafka).

### Logs centralizados (Loki/Grafana)
```
{app=~".+"} |= "<correlationId>"
→ linhas de todos os 5 serviços para a mesma operação
```

---

## 14. Estrutura das mudanças (resumo)

**contrato-service** (produtor)
- `domain/event/`: `EntregaRegistrada`, `ContratoConcluido`, `ContratoCancelado` (novos).
- `domain/model/Contrato`: transições agora registram eventos.
- `application/ContratoApplicationService`: casos de uso entrega/conclusão/cancelamento + gravação no Outbox.
- `infrastructure/outbox/`: `OutboxEvent`, `OutboxEventRepository`, `EventEnvelope`, `OutboxTopics`, `OutboxRecorder`, `OutboxRelay`.
- `infrastructure/kafka/KafkaTopicsConfig`: tópicos com 3 partições.
- `infrastructure/web/`: `CorrelationIdFilter`, `CorrelationIdContext`, endpoints novos.

**notificacao / reputacao / auditoria** (consumidores)
- `EventEnvelope`, `ProcessedEvent` (+repo), `<Servico>Listener` (`@KafkaListener` idempotente),
  `KafkaErrorHandlingConfig` (retry + DLQ).

**Todos os 5 serviços do fluxo**
- `logback-spring.xml` (console + Loki), tracing (Micrometer + Zipkin), MDC com correlation/trace.

**infra**
- `docker-compose.yml`: + zipkin, loki, grafana.
- `grafana/provisioning/datasources/datasources.yml`: Loki + Zipkin.
