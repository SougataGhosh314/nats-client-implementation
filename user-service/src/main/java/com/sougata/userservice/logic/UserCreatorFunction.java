package com.sougata.userservice.logic;

import com.sougata.natscore.contract.PayloadFunction;
import com.sougata.natscore.model.PayloadWrapper;
import com.sougata.userprotos.UserRequest;
import com.sougata.userprotos.UserResponse;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class UserCreatorFunction implements PayloadFunction {

    @Override
    public PayloadWrapper<byte[]> process(PayloadWrapper<byte[]> request) {
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

            return request.toBuilder()
                    .setPayload(response.toByteArray())
                    .setPayloadType(UserResponse.class.getName())
                    .build();

        } catch (Exception e) {
            throw new RuntimeException("Failed to process user creation request", e);
        }
    }
}

