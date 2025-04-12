package com.sougata.natscore.dispatcher;

import com.sougata.natscore.config.EventComponentConfig;
import com.sougata.natscore.config.EventComponentEntry;
import com.sougata.natscore.config.TopicBinding;
import com.sougata.natscore.contract.PayloadSupplier;
import com.sougata.natscore.model.PayloadHeader;
import com.sougata.natscore.model.PayloadWrapper;
import io.nats.client.Connection;
import io.nats.client.impl.Headers;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class SupplierDispatcher {
    private final Connection connection;
    private final Map<String, String> writeTopicMap;

    public SupplierDispatcher(Connection connection, EventComponentConfig config) {
        this.connection = connection;
        this.writeTopicMap = new HashMap<>();

        for (EventComponentEntry entry : config.getComponents()) {
            if ("supplier".equals(entry.getType())) {
                for (TopicBinding binding : entry.getWriteTopics()) {
                    writeTopicMap.put(binding.getMessageType(), binding.getTopicName());
                }
            }
        }
    }

    public void register(PayloadSupplier supplier) {
        new Thread(() -> {
            while (true) {
                try {
                    PayloadWrapper<byte[]> payload = supplier.supply();
                    if (payload != null) {
                        String payloadType = payload.getHeader(PayloadHeader.PAYLOAD_TYPE);
                        String targetTopic = writeTopicMap.get(payloadType);
                        if (targetTopic != null) {
                            connection.publish(targetTopic, toHeaders(payload), payload.getPayload());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private Headers toHeaders(PayloadWrapper<byte[]> wrapper) {
        Headers headers = new Headers();
        wrapper.getPayloadHeaders().forEach((k, v) -> headers.add(k.name(), v));
        return headers;
    }
}