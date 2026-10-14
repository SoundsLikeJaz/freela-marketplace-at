package br.com.freela.contrato.infrastructure.outbox;

import br.com.freela.contrato.domain.event.*;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.web.CorrelationIdContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class OutboxRecorder {

    private static final Logger log = LoggerFactory.getLogger(OutboxRecorder.class);

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;
    private final OutboxTopics topics;

    public OutboxRecorder(OutboxEventRepository repository, ObjectMapper objectMapper, OutboxTopics topics) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.topics = topics;
    }

    public void gravar(DomainEvent event) {
        UUID contratoId = extrairContratoId(event);
        String topico = topics.topicoDe(event.eventType());
        String correlationId = CorrelationIdContext.get();

        JsonNode data = objectMapper.valueToTree(event);
        EventEnvelope envelope = new EventEnvelope(
                event.eventId(), event.eventType(), contratoId, event.occurredAt(), correlationId, data);

        String payload;
        try {
            payload = objectMapper.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao serializar evento " + event.eventType() + " " + event.eventId(), e);
        }

        OutboxEvent outbox = new OutboxEvent(
                event.eventId(), event.eventType(), topico, contratoId.toString(),
                contratoId, correlationId, payload);
        repository.save(outbox);

        log.info("outbox.gravado contratoId={} eventId={} eventType={} topico={} correlationId={}",
                contratoId, event.eventId(), event.eventType(), topico, correlationId);
    }

    private UUID extrairContratoId(DomainEvent event) {
        return switch (event) {
            case ContratoCriado e -> e.contratoId();
            case EntregaRegistrada e -> e.contratoId();
            case ContratoConcluido e -> e.contratoId();
            case ContratoCancelado e -> e.contratoId();
            default -> throw new IllegalArgumentException("Evento sem contratoId conhecido: " + event.eventType());
        };
    }
}
