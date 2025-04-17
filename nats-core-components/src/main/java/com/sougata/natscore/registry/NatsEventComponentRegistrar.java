package com.sougata.natscore.registry;

import com.sougata.natscore.config.EventComponentConfig;
import com.sougata.natscore.config.EventComponentEntry;
import com.sougata.natscore.contract.*;
import com.sougata.natscore.dispatcher.*;
import com.sougata.natscore.enums.HandlerType;
import org.springframework.stereotype.Component;

@Component
public class NatsEventComponentRegistrar {
    public NatsEventComponentRegistrar(
            EventComponentConfig config, org.springframework.context.ApplicationContext context,
            ConsumerDispatcher consumerDispatcher,
            FunctionDispatcher functionDispatcher,
            FunctionFanoutDispatcher functionFanoutDispatcher,
            SupplierDispatcher supplierDispatcher,
            SupplierFanoutDispatcher supplierFanoutDispatcher
    ) throws ClassNotFoundException {

        for (EventComponentEntry entry : config.getComponents()) {
            Object bean = context.getBean(Class.forName(entry.getHandlerClass()));

            switch (entry.getHandlerType()) {
                case FUNCTION -> functionDispatcher.register(entry.getReadTopics(), (PayloadFunction) bean);
                case FUNCTION_FANOUT -> functionFanoutDispatcher.register(entry.getReadTopics(), (PayloadFunctionFanout) bean);
                case CONSUMER -> consumerDispatcher.register(entry.getReadTopics(), (PayloadConsumer) bean);
                case SUPPLIER -> supplierDispatcher.register((PayloadSupplier) bean);
                case SUPPLIER_FANOUT -> supplierFanoutDispatcher.register((PayloadSupplierFanout) bean);
                default -> throw new IllegalArgumentException("Unknown type: " + entry.getHandlerType());
            }
        }
    }
}
