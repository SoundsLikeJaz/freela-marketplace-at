package br.com.freela.contrato.infrastructure.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int LOTE = 50;

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(OutboxEventRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${contrato.outbox.relay-intervalo-ms:2000}")
    @Transactional
    public void publicarPendentes() {
        List<OutboxEvent> pendentes = repository.findByStatusOrderByCriadoEmAsc(
                OutboxEvent.Status.PENDENTE, Limit.of(LOTE));
        if (pendentes.isEmpty()) {
            return;
        }
        log.info("outbox.relay.ciclo.inicio pendentes={}", pendentes.size());
        int publicados = 0;
        for (OutboxEvent evento : pendentes) {
            try {
                MDC.put("correlationId", evento.getCorrelationId());
                MDC.put("contratoId", evento.getContratoId().toString());
                MDC.put("eventId", evento.getEventId().toString());

                var record = new ProducerRecord<>(
                        evento.getTopico(), null, evento.getChaveParticao(), evento.getPayload());
                record.headers().add(new RecordHeader("eventId",
                        evento.getEventId().toString().getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader("eventType",
                        evento.getTipo().getBytes(StandardCharsets.UTF_8)));
                if (evento.getCorrelationId() != null) {
                    record.headers().add(new RecordHeader("X-Correlation-Id",
                            evento.getCorrelationId().getBytes(StandardCharsets.UTF_8)));
                }

                evento.registrarTentativa();
                kafkaTemplate.send(record).get();
                evento.marcarPublicado();
                publicados++;
                log.info("outbox.publicado eventId={} eventType={} topico={} chave={}",
                        evento.getEventId(), evento.getTipo(), evento.getTopico(), evento.getChaveParticao());
            } catch (Exception e) {
                log.error("outbox.publicacao.falha eventId={} eventType={} tentativas={} erro={}",
                        evento.getEventId(), evento.getTipo(), evento.getTentativas(), e.getMessage());
            } finally {
                MDC.clear();
            }
        }
        repository.saveAll(pendentes);
        log.info("outbox.relay.ciclo.fim publicados={} total={}", publicados, pendentes.size());
    }
}
