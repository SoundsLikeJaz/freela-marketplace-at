package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.outbox.OutboxRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final OutboxRecorder outboxRecorder;

    public ContratoApplicationService(ContratoRepository repository, OutboxRecorder outboxRecorder) {
        this.repository = repository;
        this.outboxRecorder = outboxRecorder;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());
        Contrato salvo = repository.salvar(contrato);
        publicarEventosPendentes(contrato);

        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        log.info("contrato.entrega.inicio contratoId={}", id);
        Contrato contrato = repository.buscarPorId(id)
                .orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        contrato.registrarEntrega();
        Contrato salvo = repository.salvar(contrato);
        publicarEventosPendentes(contrato);
        log.info("contrato.entrega.sucesso contratoId={} status={}", salvo.id(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato concluir(UUID id) {
        log.info("contrato.conclusao.inicio contratoId={}", id);
        Contrato contrato = repository.buscarPorId(id)
                .orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        contrato.concluir();
        Contrato salvo = repository.salvar(contrato);
        publicarEventosPendentes(contrato);
        log.info("contrato.conclusao.sucesso contratoId={} status={}", salvo.id(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato cancelar(UUID id, String motivo) {
        log.info("contrato.cancelamento.inicio contratoId={} motivo={}", id, motivo);
        Contrato contrato = repository.buscarPorId(id)
                .orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        contrato.cancelar(motivo);
        Contrato salvo = repository.salvar(contrato);
        publicarEventosPendentes(contrato);
        log.info("contrato.cancelamento.sucesso contratoId={} status={}", salvo.id(), salvo.status());
        return salvo;
    }

    private void publicarEventosPendentes(Contrato contrato) {
        for (DomainEvent event : contrato.pullDomainEvents()) {
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={} occurredAt={}",
                    contrato.id(), event.eventId(), event.eventType(), event.occurredAt());
            outboxRecorder.gravar(event);
        }
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }
}
