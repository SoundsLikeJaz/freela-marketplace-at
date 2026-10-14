package br.com.freela.contrato.infrastructure.web;

import jakarta.validation.constraints.Size;

public record CancelarContratoRequest(
        @Size(max = 500) String motivo) {}
