package br.com.freela.reputacao;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class ReputacaoListener {

    private static final Logger log = LoggerFactory.getLogger(ReputacaoListener.class);

    private final ObjectMapper objectMapper;
    private final ReputacaoService reputacaoService;
    private final ProcessedEventRepository processedEvents;

    public ReputacaoListener(ObjectMapper objectMapper, ReputacaoService reputacaoService,
                             ProcessedEventRepository processedEvents) {
        this.objectMapper = objectMapper;
        this.reputacaoService = reputacaoService;
        this.processedEvents = processedEvents;
    }

    @KafkaListener(topics = "contratos.contrato-concluido", groupId = "reputacao-service")
    @Transactional
    public void consumir(String mensagem) throws Exception {
        EventEnvelope envelope = objectMapper.readValue(mensagem, EventEnvelope.class);
        try {
            MDC.put("correlationId", envelope.correlationId());
            MDC.put("contratoId", String.valueOf(envelope.contratoId()));
            MDC.put("eventId", String.valueOf(envelope.eventId()));

            log.info("reputacao.evento.recebido eventId={} eventType={} contratoId={} correlationId={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());

            if (processedEvents.existsById(envelope.eventId())) {
                log.info("reputacao.evento.duplicado.ignorado eventId={} eventType={}",
                        envelope.eventId(), envelope.eventType());
                return;
            }

            JsonNode data = envelope.data();
            UUID freelancerId = UUID.fromString(data.get("freelancerId").asText());
            BigDecimal valor = new BigDecimal(data.get("valor").asText());

            reputacaoService.registrarContratoConcluido(envelope.contratoId(), freelancerId, valor);

            processedEvents.save(new ProcessedEvent(envelope.eventId(), envelope.eventType()));
            log.info("reputacao.evento.processado eventId={} contratoId={} freelancerId={} resultado=OK",
                    envelope.eventId(), envelope.contratoId(), freelancerId);
        } finally {
            MDC.clear();
        }
    }
}
