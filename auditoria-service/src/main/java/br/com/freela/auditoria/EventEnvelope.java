package br.com.freela.auditoria;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/**
 * Espelho do envelope publicado pelo contrato-service.
 * Representa o contrato de comunicação consumido por este serviço.
 */
record EventEnvelope(
        UUID eventId,
        String eventType,
        UUID contratoId,
        Instant occurredAt,
        String correlationId,
        JsonNode data) {
}
