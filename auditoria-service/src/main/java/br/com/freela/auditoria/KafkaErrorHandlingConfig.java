package br.com.freela.auditoria;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Value("${contrato.consumo.max-tentativas:3}")
    private long maxTentativas;

    @Value("${contrato.consumo.intervalo-retry-ms:1000}")
    private long intervaloRetryMs;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    ProducerFactory<String, String> dltProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    KafkaTemplate<String, String> dltKafkaTemplate(ProducerFactory<String, String> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> dltKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dltKafkaTemplate,
                (record, ex) -> {
                    log.error("consumo.dlt.envio topico={} particao={} offset={} erro={}",
                            record.topic(), record.partition(), record.offset(), ex.getMessage());
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                });

        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer, new FixedBackOff(intervaloRetryMs, maxTentativas - 1));
        handler.setCommitRecovered(true);
        return handler;
    }
}
