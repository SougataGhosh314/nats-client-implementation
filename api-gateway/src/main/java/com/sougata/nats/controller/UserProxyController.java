package com.sougata.nats.controller;

import com.sougata.userprotos.UserRequest;
import com.sougata.userprotos.UserResponse;
import com.sougata.userprotos.UserEvent;
import com.sougata.userprotos.UserActivity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sougata.natscore.model.PayloadWrapper;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.impl.Headers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/api/user")
public class UserProxyController {

    @Value("${nats.url}")
    private String natsUrl;

    @Value("${user.topics.create}")
    private String userCreateTopic;

    @Value("${user.topics.response}")
    private String userResponseTopic;

    @Value("${user.topics.audit}")
    private String auditTopic;

    @Value("${user.topics.activity}")
    private String activityTopic;


    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping("/create")
    public Map<String, String> createUser(@RequestBody Map<String, String> userInput) {
        try (Connection connection = Nats.connect(natsUrl)) {

            // Build the request proto
            UserRequest request = UserRequest.newBuilder()
                    .putAllBody(userInput)
                    .build();

            // Wrap in PayloadWrapper
            PayloadWrapper<byte[]> wrappedRequest = new PayloadWrapper<>(request.toByteArray(), "com.sougata.protos.UserRequest");

            // Send and wait for response
            Message reply = connection.request(userCreateTopic,
                    toHeaders(wrappedRequest),
                    wrappedRequest.getPayload(),
                    Duration.ofSeconds(2));

            // Extract and unwrap
            PayloadWrapper<byte[]> wrappedResponse = new PayloadWrapper<>(reply.getData(), "com.sougata.protos.UserResponse");
            UserResponse response = UserResponse.parseFrom(wrappedResponse.getPayload());

            return response.getBodyMap();

        } catch (Exception e) {
            e.printStackTrace();
            return Map.of("error", "Failed: " + e.getMessage());
        }
    }

    @PostMapping("/audit/event")
    public ResponseEntity<String> logUserEvent(@RequestBody UserEvent event) {
        return sendAuditMessage(event.toByteArray(), "com.sougata.protos.UserEvent");
    }

    @PostMapping("/audit/activity")
    public ResponseEntity<String> logUserActivity(@RequestBody UserActivity activity) {
        return sendAuditMessage(activity.toByteArray(), "com.sougata.protos.UserActivity");
    }

    private ResponseEntity<String> sendAuditMessage(byte[] payload, String payloadType) {
        try (Connection connection = Nats.connect(natsUrl)) {
            PayloadWrapper<byte[]> wrapper = new PayloadWrapper<>(payload, payloadType);
            connection.publish(auditTopic, toHeaders(wrapper), wrapper.getPayload());
            return ResponseEntity.accepted().body("Audit message sent.");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to send audit message.");
        }
    }

    private Headers toHeaders(PayloadWrapper<byte[]> wrapper) {
        Headers headers = new Headers();
        wrapper.getPayloadHeaders().forEach((key, value) -> headers.add(key.name(), value));
        return headers;
    }
}
