package br.com.freela.reputacao;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "processed_events")
class ProcessedEvent {
    @Id
    UUID eventId;
    String eventType;
    Instant processadoEm;

    protected ProcessedEvent() {}

    ProcessedEvent(UUID eventId, String eventType) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.processadoEm = Instant.now();
    }
}
