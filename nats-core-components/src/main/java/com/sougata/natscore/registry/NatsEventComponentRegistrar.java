package com.sougata.natscore.registry;

import com.sougata.natscore.config.EventComponentConfig;
import com.sougata.natscore.config.EventComponentEntry;
import com.sougata.natscore.contract.PayloadConsumer;
import com.sougata.natscore.contract.PayloadFunction;
import com.sougata.natscore.contract.PayloadSupplier;
import com.sougata.natscore.dispatcher.ConsumerDispatcher;
import com.sougata.natscore.dispatcher.FunctionDispatcher;
import com.sougata.natscore.dispatcher.SupplierDispatcher;
import org.springframework.stereotype.Component;

@Component
public class NatsEventComponentRegistrar {
    public NatsEventComponentRegistrar(
            EventComponentConfig config, org.springframework.context.ApplicationContext context,
            ConsumerDispatcher consumerDispatcher,
            FunctionDispatcher functionDispatcher,
            SupplierDispatcher supplierDispatcher
    ) throws ClassNotFoundException {

        for (EventComponentEntry entry : config.getComponents()) {
            Object bean = context.getBean(Class.forName(entry.getHandlerClass()));

            switch (entry.getType()) {
                case "function" -> functionDispatcher.register(entry.getReadTopics(), (PayloadFunction) bean);
                case "consumer" -> consumerDispatcher.register(entry.getReadTopics(), (PayloadConsumer) bean);
                case "supplier" -> supplierDispatcher.register((PayloadSupplier) bean);
                default -> throw new IllegalArgumentException("Unknown type: " + entry.getType());
            }
        }
    }
}
