package br.com.freela.notificacao;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
public class NotificacaoListener {

    private static final Logger log = LoggerFactory.getLogger(NotificacaoListener.class);

    private final ObjectMapper objectMapper;
    private final NotificacaoService notificacaoService;
    private final ProcessedEventRepository processedEvents;

    public NotificacaoListener(ObjectMapper objectMapper, NotificacaoService notificacaoService,
                               ProcessedEventRepository processedEvents) {
        this.objectMapper = objectMapper;
        this.notificacaoService = notificacaoService;
        this.processedEvents = processedEvents;
    }

    @KafkaListener(
            topics = {
                    "contratos.contrato-criado",
                    "contratos.entrega-registrada",
                    "contratos.contrato-concluido",
                    "contratos.contrato-cancelado"
            },
            groupId = "notificacao-service")
    @Transactional
    public void consumir(String mensagem) throws Exception {
        EventEnvelope envelope = objectMapper.readValue(mensagem, EventEnvelope.class);
        try {
            MDC.put("correlationId", envelope.correlationId());
            MDC.put("contratoId", String.valueOf(envelope.contratoId()));
            MDC.put("eventId", String.valueOf(envelope.eventId()));

            log.info("notificacao.evento.recebido eventId={} eventType={} contratoId={} correlationId={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());

            if (processedEvents.existsById(envelope.eventId())) {
                log.info("notificacao.evento.duplicado.ignorado eventId={} eventType={}",
                        envelope.eventId(), envelope.eventType());
                return;
            }

            JsonNode data = envelope.data();
            UUID clienteId = uuid(data, "clienteId");
            UUID freelancerId = uuid(data, "freelancerId");

            UUID destinatario;
            String mensagemTexto;
            switch (envelope.eventType()) {
                case "ContratoCriado" -> {
                    destinatario = freelancerId;
                    mensagemTexto = "Novo contrato criado para você: " + envelope.contratoId();
                }
                case "EntregaRegistrada" -> {
                    destinatario = clienteId;
                    mensagemTexto = "A entrega do contrato " + envelope.contratoId() + " foi registrada";
                }
                case "ContratoConcluido" -> {
                    destinatario = freelancerId;
                    mensagemTexto = "O contrato " + envelope.contratoId() + " foi concluído";
                }
                case "ContratoCancelado" -> {
                    destinatario = clienteId;
                    mensagemTexto = "O contrato " + envelope.contratoId() + " foi cancelado";
                }
                default -> {
                    log.warn("notificacao.evento.ignorado.tipoDesconhecido eventType={}", envelope.eventType());
                    return;
                }
            }

            log.info("notificacao.registro.destinatario eventId={} destinatario={}", envelope.eventId(), destinatario);
            notificacaoService.registrar(envelope.contratoId(), destinatario, envelope.eventType(), mensagemTexto);

            processedEvents.save(new ProcessedEvent(envelope.eventId(), envelope.eventType()));
            log.info("notificacao.evento.processado eventId={} eventType={} contratoId={} resultado=OK",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId());
        } finally {
            MDC.clear();
        }
    }

    private UUID uuid(JsonNode data, String campo) {
        JsonNode n = data.get(campo);
        return (n == null || n.isNull()) ? null : UUID.fromString(n.asText());
    }
}
