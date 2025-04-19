package com.sougata.userservice.logic;

import com.sougata.natscore.contract.PayloadConsumer;
import com.sougata.natscore.model.PayloadWrapper;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
public class UserAuditConsumer implements PayloadConsumer {

    @Override
    public void consume(PayloadWrapper<byte[]> message) {
        String auditMessage = new String(message.getPayload(), StandardCharsets.UTF_8);
        Map<?, ?> headers = message.getPayloadHeaders();

        System.out.println("Audit Log: " + auditMessage);
        System.out.println("Headers: " + headers);
    }
}

