package br.com.freela.auditoria;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {}
