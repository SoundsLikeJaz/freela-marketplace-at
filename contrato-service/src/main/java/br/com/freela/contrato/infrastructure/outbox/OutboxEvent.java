package br.com.freela.contrato.infrastructure.outbox;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events", indexes = {
        @Index(name = "idx_outbox_status_criado", columnList = "status,criadoEm")
})
public class OutboxEvent {

    public enum Status { PENDENTE, PUBLICADO }

    @Id
    private UUID eventId;

    @Column(nullable = false, length = 60)
    private String tipo;

    @Column(nullable = false, length = 120)
    private String topico;

    @Column(nullable = false, length = 60)
    private String chaveParticao;

    @Column(nullable = false)
    private UUID contratoId;

    @Column(length = 120)
    private String correlationId;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Status status;

    @Column(nullable = false)
    private Instant criadoEm;

    private Instant publicadoEm;

    @Column(nullable = false)
    private int tentativas;

    protected OutboxEvent() {}

    public OutboxEvent(UUID eventId, String tipo, String topico, String chaveParticao,
                       UUID contratoId, String correlationId, String payload) {
        this.eventId = eventId;
        this.tipo = tipo;
        this.topico = topico;
        this.chaveParticao = chaveParticao;
        this.contratoId = contratoId;
        this.correlationId = correlationId;
        this.payload = payload;
        this.status = Status.PENDENTE;
        this.criadoEm = Instant.now();
        this.tentativas = 0;
    }

    public void marcarPublicado() {
        this.status = Status.PUBLICADO;
        this.publicadoEm = Instant.now();
    }

    public void registrarTentativa() {
        this.tentativas++;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getTipo() {
        return tipo;
    }

    public String getTopico() {
        return topico;
    }

    public String getChaveParticao() {
        return chaveParticao;
    }

    public UUID getContratoId() {
        return contratoId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getPayload() {
        return payload;
    }

    public Status getStatus() {
        return status;
    }

    public Instant getCriadoEm() {
        return criadoEm;
    }

    public Instant getPublicadoEm() {
        return publicadoEm;
    }

    public int getTentativas() {
        return tentativas;
    }
}
