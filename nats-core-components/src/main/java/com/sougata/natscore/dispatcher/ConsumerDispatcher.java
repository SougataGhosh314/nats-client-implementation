package com.sougata.natscore.dispatcher;

import com.sougata.natscore.config.TopicBinding;
import com.sougata.natscore.contract.PayloadConsumer;
import com.sougata.natscore.enums.MDCLoggingEnum;
import com.sougata.natscore.model.PayloadHeader;
import com.sougata.natscore.model.PayloadWrapper;
import com.sougata.natscore.monitoring.NatsMetricsRecorder;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.impl.Headers;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.sougata.natscore.util.NatsUtil.headersToMap;

@Slf4j
@Component
public class ConsumerDispatcher {
    private final Connection connection;
    private final NatsMetricsRecorder metricsRecorder;

    public ConsumerDispatcher(Connection connection, NatsMetricsRecorder metricsRecorder) {
        this.connection = connection;
        this.metricsRecorder = metricsRecorder;
    }

    public void register(List<TopicBinding> topics, PayloadConsumer handler) {
        for (TopicBinding binding : topics) {
            Dispatcher dispatcher = connection.createDispatcher(msg -> {
                PayloadWrapper<byte[]> incoming = PayloadWrapper.<byte[]>newBuilder()
                        .setPayload(msg.getData())
                        .setPayloadType(binding.getMessageType())
                        .setCorrelationId(msg.getHeaders().getFirst(PayloadHeader.CORRELATION_ID.getKey()))
                        .setCreationTimestamp(msg.getHeaders().getFirst(PayloadHeader.CREATION_TS.getKey()))
                        .build();

                MDC.put(MDCLoggingEnum.CORRELATION_ID.getLoggingKey(), msg.getHeaders().getFirst(PayloadHeader.CORRELATION_ID.getKey()));
                logIncomingMessage(binding.getTopicName(), msg.getHeaders());
                metricsRecorder.incrementReceived(binding.getTopicName()); // record metrics

                try {
                    handler.consume(incoming);
                } catch (Exception e) {
                    log.error("Error while consuming message: ", e);
                    metricsRecorder.incrementError(binding.getTopicName());
                } finally {
                    MDC.remove(MDCLoggingEnum.CORRELATION_ID.getLoggingKey()); // ✅ safer than MDC.clear()
                }
            });

            if (StringUtils.isEmpty(binding.getQueueGroup()))
                dispatcher.subscribe(binding.getTopicName());
            else dispatcher.subscribe(binding.getTopicName(), binding.getQueueGroup());
        }
    }

    private static void logIncomingMessage(String topicName, Headers headers) {
        log.debug("Received message on topic: {}", topicName);
        log.trace("Message headers: {}", headersToMap(headers));
    }
}