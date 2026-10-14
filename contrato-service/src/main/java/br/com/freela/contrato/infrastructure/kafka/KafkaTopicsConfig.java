package br.com.freela.contrato.infrastructure.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    private static final int PARTICOES = 3;
    private static final int REPLICAS = 1;

    @Bean
    NewTopic contratoCriadoTopic() {
        return TopicBuilder.name("contratos.contrato-criado").partitions(PARTICOES).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic entregaRegistradaTopic() {
        return TopicBuilder.name("contratos.entrega-registrada").partitions(PARTICOES).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic contratoConcluidoTopic() {
        return TopicBuilder.name("contratos.contrato-concluido").partitions(PARTICOES).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic contratoCanceladoTopic() {
        return TopicBuilder.name("contratos.contrato-cancelado").partitions(PARTICOES).replicas(REPLICAS).build();
    }
}
