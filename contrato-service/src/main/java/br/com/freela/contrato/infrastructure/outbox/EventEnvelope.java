package br.com.freela.contrato.infrastructure.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String eventType,
        UUID contratoId,
        Instant occurredAt,
        String correlationId,
        JsonNode data) {
}
