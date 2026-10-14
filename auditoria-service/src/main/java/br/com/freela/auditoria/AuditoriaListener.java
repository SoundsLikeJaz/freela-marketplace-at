package br.com.freela.auditoria;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class AuditoriaListener {

    private static final Logger log = LoggerFactory.getLogger(AuditoriaListener.class);

    private final ObjectMapper objectMapper;
    private final AuditoriaService auditoriaService;
    private final ProcessedEventRepository processedEvents;

    public AuditoriaListener(ObjectMapper objectMapper, AuditoriaService auditoriaService,
                             ProcessedEventRepository processedEvents) {
        this.objectMapper = objectMapper;
        this.auditoriaService = auditoriaService;
        this.processedEvents = processedEvents;
    }

    @KafkaListener(
            topics = {
                    "contratos.contrato-criado",
                    "contratos.entrega-registrada",
                    "contratos.contrato-concluido",
                    "contratos.contrato-cancelado"
            },
            groupId = "auditoria-service")
    @Transactional
    public void consumir(String mensagem) throws Exception {
        EventEnvelope envelope = objectMapper.readValue(mensagem, EventEnvelope.class);
        try {
            MDC.put("correlationId", envelope.correlationId());
            MDC.put("contratoId", String.valueOf(envelope.contratoId()));
            MDC.put("eventId", String.valueOf(envelope.eventId()));

            log.info("auditoria.evento.recebido eventId={} eventType={} contratoId={} correlationId={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());

            if (processedEvents.existsById(envelope.eventId())) {
                log.info("auditoria.evento.duplicado.ignorado eventId={} eventType={}",
                        envelope.eventId(), envelope.eventType());
                return;
            }

            auditoriaService.registrar(
                    envelope.eventId(),
                    envelope.contratoId(),
                    envelope.eventType(),
                    envelope.correlationId(),
                    mensagem);

            processedEvents.save(new ProcessedEvent(envelope.eventId(), envelope.eventType()));
            log.info("auditoria.evento.processado eventId={} eventType={} contratoId={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId());
        } finally {
            MDC.clear();
        }
    }
}
