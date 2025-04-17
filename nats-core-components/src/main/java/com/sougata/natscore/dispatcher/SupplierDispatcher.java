package com.sougata.natscore.dispatcher;

import com.sougata.natscore.config.EventComponentConfig;
import com.sougata.natscore.config.EventComponentEntry;
import com.sougata.natscore.config.TopicBinding;
import com.sougata.natscore.contract.PayloadSupplier;
import com.sougata.natscore.enums.HandlerType;
import com.sougata.natscore.model.PayloadWrapper;
import com.sougata.natscore.monitoring.NatsMetricsRecorder;
import io.nats.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Slf4j
@Component
public class SupplierDispatcher extends AbstractDispatcher {

    public SupplierDispatcher(Connection connection, EventComponentConfig config, NatsMetricsRecorder metricsRecorder) {
        super(connection, metricsRecorder);
        this.writeTopicMap = new HashMap<>();

        config.getComponents().stream()
                .filter(entry -> HandlerType.SUPPLIER.equals(entry.getHandlerType()))
                .flatMap(entry -> entry.getWriteTopics().stream())
                .forEach(binding -> writeTopicMap.put(binding.getMessageType(), binding.getTopicName()));
    }

    public void register(PayloadSupplier supplier) {
        new Thread(() -> {
            while (true) {
                PayloadWrapper<byte[]> payload = null;
                try {
                    payload = supplier.supply();
                } catch (Exception e) {
                    log.error("Error in PayloadSupplier: {}.supply(): ", supplier.getClass().getName(), e);
                }
                if (payload != null) publish(payload);
            }
        }).start();
    }
}