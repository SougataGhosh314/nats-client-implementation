package com.sougata.natscore.dispatcher;

import com.sougata.natscore.config.TopicBinding;
import com.sougata.natscore.contract.PayloadConsumer;
import com.sougata.natscore.model.PayloadHeader;
import com.sougata.natscore.model.PayloadWrapper;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ConsumerDispatcher {
    private final Connection connection;

    public ConsumerDispatcher(Connection connection) {
        this.connection = connection;
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
                handler.consume(incoming);
            });

            if (StringUtils.isEmpty(binding.getQueueGroup()))
                dispatcher.subscribe(binding.getTopicName());
            else dispatcher.subscribe(binding.getTopicName(), binding.getQueueGroup());
        }
    }
}