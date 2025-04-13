package com.sougata.nats.controller;

import com.google.protobuf.util.JsonFormat;
import com.sougata.nats.registry.CorrelationRegistry;
import com.sougata.natscore.model.PayloadHeader;
import com.sougata.natscore.model.PayloadWrapper;
import com.sougata.userprotos.UserActivity;
import com.sougata.userprotos.UserEvent;
import com.sougata.userprotos.UserRequest;
import com.sougata.userprotos.UserResponse;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/user")
public class UserProxyController {
    private final Connection connection;
    private final CorrelationRegistry registry;

    public UserProxyController(Connection connection, CorrelationRegistry registry) {
        this.connection = connection;
        this.registry = registry;
    }

    @PostMapping("/create")
    public ResponseEntity<String> createUser(@RequestBody Map<String, String> body) throws Exception {
        UserRequest request = UserRequest.newBuilder().putAllBody(body).build();
        PayloadWrapper<byte[]> wrapper = PayloadWrapper.<byte[]>newBuilder()
                .setPayload(request.toByteArray())
                .setPayloadType(UserRequest.class.getName())
                .build();
        String correlationId = wrapper.getHeader(PayloadHeader.CORRELATION_ID);
        CompletableFuture<Message> future = registry.register(correlationId);

        // Send request to any topic
        connection.publish("user.create", toHeaders(wrapper), wrapper.getPayload());

        // Await response
        Message reply = future.get(3, TimeUnit.SECONDS);
        UserResponse response = UserResponse.parseFrom(reply.getData());
        return ResponseEntity.ok(JsonFormat.printer().print(response));
    }

    @PostMapping(value = "/audit/event")
    public ResponseEntity<String> logUserEvent(@RequestBody String jsonBody) throws IOException {
        UserEvent.Builder userEvent = UserEvent.newBuilder();
        JsonFormat.parser().merge(jsonBody, userEvent);
        return sendAuditMessage(userEvent.build().toByteArray(), UserEvent.class.getName(), "user.audit");
    }

    @PostMapping(value = "/audit/activity")
    public ResponseEntity<String> logUserActivity(@RequestBody String jsonBody) throws IOException {
        UserActivity.Builder userActivity = UserActivity.newBuilder();
        JsonFormat.parser().merge(jsonBody, userActivity);
        return sendAuditMessage(userActivity.build().toByteArray(), UserActivity.class.getName(), "user.activity");
    }

    private ResponseEntity<String> sendAuditMessage(byte[] payload, String payloadType, String auditTopic) {
        PayloadWrapper<byte[]> payloadWrapper = PayloadWrapper.<byte[]>newBuilder()
                .setPayload(payload)
                .setPayloadType(payloadType)
                .build();

        try {
            connection.publish(auditTopic, toHeaders(payloadWrapper), payloadWrapper.getPayload());
            return ResponseEntity.accepted().body("Audit message sent.");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to send audit message.");
        }
    }

    @SuppressWarnings("unchecked")
    private Headers toHeaders(PayloadWrapper wrapper) {
        Headers headers = new Headers();
        Map<PayloadHeader, String> payloadHeaders = (Map<PayloadHeader, String>) wrapper.getPayloadHeaders();
        payloadHeaders.forEach((k, v) -> headers.add(k.getKey(), v));
        return headers;
    }
}
