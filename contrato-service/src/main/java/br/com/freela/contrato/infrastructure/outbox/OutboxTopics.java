package br.com.freela.contrato.infrastructure.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.Map;

@ConfigurationProperties(prefix = "contrato.outbox")
public class OutboxTopics {

    private Map<String, String> topicos = Map.of();

    public Map<String, String> getTopicos() { return topicos; }
    public void setTopicos(Map<String, String> topicos) { this.topicos = topicos; }

    public String topicoDe(String eventType) {
        String topico = topicos.get(eventType);
        if (topico == null) {
            throw new IllegalArgumentException("Nenhum tópico configurado para o evento: " + eventType);
        }
        return topico;
    }
}
