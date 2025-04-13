package com.sougata.nats.registry;

import io.nats.client.Message;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CorrelationRegistry {
    private final Map<String, CompletableFuture<Message>> pendingResponses = new ConcurrentHashMap<>();

    public CompletableFuture<Message> register(String correlationId) {
        CompletableFuture<Message> future = new CompletableFuture<>();
        pendingResponses.put(correlationId, future);
        return future;
    }

    public void complete(String correlationId, Message message) {
        CompletableFuture<Message> future = pendingResponses.remove(correlationId);
        if (future != null) future.complete(message);
    }
}
