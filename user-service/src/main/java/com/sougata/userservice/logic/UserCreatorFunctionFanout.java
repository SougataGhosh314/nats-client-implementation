package com.sougata.userservice.logic;

import com.sougata.natscore.contract.PayloadFunctionFanout;
import com.sougata.natscore.model.PayloadWrapper;
import com.sougata.userprotos.UserRequest;
import com.sougata.userprotos.UserResponse;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class UserCreatorFunctionFanout implements PayloadFunctionFanout {

    @Override
    public List<PayloadWrapper<byte[]>> process(PayloadWrapper<byte[]> request) {
        try {
            UserRequest userRequest = UserRequest.parseFrom(request.getPayload());
            Map<String, String> requestData = userRequest.getBodyMap();

            UserResponse response = UserResponse.newBuilder()
                    .setProcessorId("user-service")
                    .putAllBody(Map.of(
                            "message", "User created successfully",
                            "inputName", requestData.getOrDefault("name", "unknown")
                    ))
                    .build();
            UserResponse anotherResponse = UserResponse.newBuilder()
                    .setProcessorId("user-service")
                    .putAllBody(Map.of(
                            "message", "Another User created successfully",
                            "inputName", requestData.getOrDefault("name", "unknown") + "'s friend"
                    ))
                    .build();

            return Arrays.asList(
                    request.toBuilder()
                            .setPayload(response.toByteArray())
                            .setPayloadType(UserResponse.class.getName())
                            .build(),
                    request.toBuilder()
                            .setPayload(anotherResponse.toByteArray())
                            .setPayloadType(UserResponse.class.getName())
                            .build()
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to process user creation request", e);
        }
    }
}

