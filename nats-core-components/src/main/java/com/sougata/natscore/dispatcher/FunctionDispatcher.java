package com.sougata.natscore.dispatcher;

import com.sougata.natscore.config.EventComponentConfig;
import com.sougata.natscore.config.EventComponentEntry;
import com.sougata.natscore.config.TopicBinding;
import com.sougata.natscore.contract.PayloadFunction;
import com.sougata.natscore.enums.MDCLoggingEnum;
import com.sougata.natscore.model.PayloadHeader;
import com.sougata.natscore.model.PayloadWrapper;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.impl.Headers;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.sougata.natscore.util.NatsUtil.headersToMap;

@Slf4j
@Component
public class FunctionDispatcher {
    private final Connection connection;
    private final Map<String, String> writeTopicMap;

    public FunctionDispatcher(Connection connection, EventComponentConfig config) {
        this.connection = connection;
        this.writeTopicMap = new HashMap<>();

        for (EventComponentEntry entry : config.getComponents()) {
            if ("function".equals(entry.getType())) {
                for (TopicBinding binding : entry.getWriteTopics()) {
                    writeTopicMap.put(binding.getMessageType(), binding.getTopicName());
                }
            }
        }
    }

    public void register(List<TopicBinding> topics, PayloadFunction handler) {
        for (TopicBinding binding : topics) {
            Dispatcher dispatcher = connection.createDispatcher(msg -> {
                PayloadWrapper<byte[]> input = PayloadWrapper.<byte[]>newBuilder()
                        .setPayload(msg.getData())
                        .setPayloadType(binding.getMessageType())
                        .setCorrelationId(msg.getHeaders().getFirst(PayloadHeader.CORRELATION_ID.getKey()))
                        .setCreationTimestamp(msg.getHeaders().getFirst(PayloadHeader.CREATION_TS.getKey()))
                        .build();

                MDC.put(MDCLoggingEnum.CORRELATION_ID.getLoggingKey(), msg.getHeaders().getFirst(PayloadHeader.CORRELATION_ID.getKey()));
                logIncomingMessage(binding.getTopicName(), msg.getHeaders());
                try {
                    PayloadWrapper<byte[]> result = handler.process(input);
                    if (result != null) {
                        String payloadType = result.getHeader(PayloadHeader.PAYLOAD_TYPE);
                        String targetTopic = writeTopicMap.get(payloadType);
                        if (targetTopic != null) {
                            logOutgoingMessage(targetTopic, toHeaders(result));
                            connection.publish(targetTopic, toHeaders(result), result.getPayload());
                        }
                    }
                } catch (Exception e) {
                    log.error("Error while processing message: ", e);
                } finally {
                    MDC.remove(MDCLoggingEnum.CORRELATION_ID.getLoggingKey()); // ✅ safer than MDC.clear()
                }

            });

            if (StringUtils.isEmpty(binding.getQueueGroup()))
                dispatcher.subscribe(binding.getTopicName());
            else dispatcher.subscribe(binding.getTopicName(), binding.getQueueGroup());
        }
    }

    private Headers toHeaders(PayloadWrapper<byte[]> wrapper) {
        Headers headers = new Headers();
        wrapper.getPayloadHeaders().forEach((k, v) -> headers.add(k.getKey(), v));
        return headers;
    }

    private static void logIncomingMessage(String topicName, Headers headers) {
        log.debug("Received message on topic: {}", topicName);
        log.trace("Message headers: {}", headersToMap(headers));
    }

    private static void logOutgoingMessage(String topicName, Headers headers) {
        log.debug("Sending message on topic: {}", topicName);
        log.trace("Message headers: {}", headersToMap(headers));
    }
}