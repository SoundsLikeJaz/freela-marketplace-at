package br.com.freela.contrato;

import br.com.freela.contrato.infrastructure.outbox.OutboxTopics;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(OutboxTopics.class)
public class ContratoApplication {
    public static void main(String[] args) { SpringApplication.run(ContratoApplication.class, args); }
}
