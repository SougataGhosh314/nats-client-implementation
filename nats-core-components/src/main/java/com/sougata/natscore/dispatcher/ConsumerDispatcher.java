package com.sougata.natscore.dispatcher;

import com.sougata.natscore.config.TopicBinding;
import com.sougata.natscore.contract.PayloadConsumer;
import com.sougata.natscore.model.PayloadWrapper;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
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
                PayloadWrapper<byte[]> wrapper = new PayloadWrapper<>(msg.getData(), binding.getMessageType());
                handler.consume(wrapper);
            });
            dispatcher.subscribe(binding.getTopicName());
        }
    }
}