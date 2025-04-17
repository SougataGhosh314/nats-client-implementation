package com.sougata.natscore.dispatcher;

import com.sougata.natscore.config.EventComponentConfig;
import com.sougata.natscore.contract.PayloadSupplierFanout;
import com.sougata.natscore.enums.HandlerType;
import com.sougata.natscore.model.PayloadWrapper;
import com.sougata.natscore.monitoring.NatsMetricsRecorder;
import io.nats.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;

@Slf4j
public class SupplierFanoutDispatcher extends AbstractDispatcher {

    public SupplierFanoutDispatcher(Connection connection, EventComponentConfig config, NatsMetricsRecorder metricsRecorder) {
        super(connection, metricsRecorder);
        this.writeTopicMap = new HashMap<>();

        config.getComponents().stream()
                .filter(entry -> HandlerType.SUPPLIER_FANOUT.equals(entry.getHandlerType()))
                .flatMap(entry -> entry.getWriteTopics().stream())
                .forEach(binding -> writeTopicMap.put(binding.getMessageType(), binding.getTopicName()));
    }

    public void register(PayloadSupplierFanout handler) {
        new Thread(() -> {
            while (true) {
                List<PayloadWrapper<byte[]>> payloadWrappers = null;
                try {
                    payloadWrappers = handler.supply();
                } catch (Exception e) {
                    log.error("Error in PayloadSupplierFanout: {}.supply(): ", handler.getClass().getName(), e);
                }
                if (CollectionUtils.isEmpty(payloadWrappers)) {
                    log.info("SupplierFanout: {} returned no payloads. Nothing to dispatch.", handler.getClass().getName());
                    return;
                }

                for (PayloadWrapper<byte[]> payloadWrapper : payloadWrappers) {
                    publish(payloadWrapper);
                }
            }
        }).start();
    }
}
