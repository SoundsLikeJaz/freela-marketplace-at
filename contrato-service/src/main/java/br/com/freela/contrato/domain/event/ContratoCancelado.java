package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.time.Instant;
import java.util.UUID;

public record ContratoCancelado(UUID eventId, Instant occurredAt, UUID contratoId,
                                UUID clienteId, UUID freelancerId, String motivo)
        implements DomainEvent {
    public static ContratoCancelado novo(Contrato c, String motivo) {
        return new ContratoCancelado(UUID.randomUUID(), Instant.now(), c.id(), c.clienteId(), c.freelancerId(), motivo);
    }
    @Override public String eventType() { return "ContratoCancelado"; }
}
