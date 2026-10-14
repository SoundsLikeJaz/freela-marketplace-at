package br.com.freela.contrato.infrastructure.outbox;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findByStatusOrderByCriadoEmAsc(OutboxEvent.Status status, Limit limit);
}
