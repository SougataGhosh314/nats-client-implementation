package com.sougata.natscore.dispatcher;

import com.sougata.natscore.config.EventComponentConfig;
import com.sougata.natscore.config.EventComponentEntry;
import com.sougata.natscore.config.TopicBinding;
import com.sougata.natscore.contract.PayloadSupplier;
import com.sougata.natscore.enums.MDCLoggingEnum;
import com.sougata.natscore.model.PayloadHeader;
import com.sougata.natscore.model.PayloadWrapper;
import io.nats.client.Connection;
import io.nats.client.impl.Headers;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static com.sougata.natscore.util.NatsUtil.headersToMap;

@Slf4j
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
                            MDC.put(MDCLoggingEnum.CORRELATION_ID.getLoggingKey(), payload.getHeader(PayloadHeader.CORRELATION_ID));
                            logOutgoingMessage(targetTopic, toHeaders(payload));
                            connection.publish(targetTopic, toHeaders(payload), payload.getPayload());
                        }
                    }
                } catch (Exception e) {
                    log.error("Error while supplying message: ", e);
                } finally {
                    MDC.remove(MDCLoggingEnum.CORRELATION_ID.getLoggingKey()); // ✅ safer than MDC.clear()
                }
            }
        }).start();
    }

    private Headers toHeaders(PayloadWrapper<byte[]> wrapper) {
        Headers headers = new Headers();
        wrapper.getPayloadHeaders().forEach((k, v) -> headers.add(k.getKey(), v));
        return headers;
    }

    private static void logOutgoingMessage(String topicName, Headers headers) {
        log.debug("Sending message on topic: {}", topicName);
        log.trace("Message headers: {}", headersToMap(headers));
    }
}