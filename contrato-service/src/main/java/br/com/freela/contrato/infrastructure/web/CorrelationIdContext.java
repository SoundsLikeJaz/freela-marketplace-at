package br.com.freela.contrato.infrastructure.web;

public final class CorrelationIdContext {

    private static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();

    private CorrelationIdContext() {}

    public static void set(String correlationId) {
        CONTEXT.set(correlationId);
    }

    public static String get() {
        return CONTEXT.get();
    }

    public static void clear() {
        CONTEXT.remove();
    }
}
