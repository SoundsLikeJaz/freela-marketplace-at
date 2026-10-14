package br.com.freela.reputacao;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

record EventEnvelope(
        UUID eventId,
        String eventType,
        UUID contratoId,
        Instant occurredAt,
        String correlationId,
        JsonNode data) {
}
