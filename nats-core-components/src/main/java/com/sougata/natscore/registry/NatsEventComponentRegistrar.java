package com.sougata.natscore.registry;

import com.sougata.natscore.config.EventComponentConfig;
import com.sougata.natscore.config.EventComponentEntry;
import com.sougata.natscore.contract.*;
import com.sougata.natscore.dispatcher.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class NatsEventComponentRegistrar {
    public NatsEventComponentRegistrar(
            EventComponentConfig config, org.springframework.context.ApplicationContext context,
            ObjectProvider<ConsumerDispatcher> consumerDispatcherProvider,
            ObjectProvider<SupplierDispatcher> supplierDispatcherProvider,
            ObjectProvider<SupplierFanoutDispatcher> supplierFanoutDispatcherProvider,
            ObjectProvider<FunctionDispatcher> functionDispatcherProvider,
            ObjectProvider<FunctionFanoutDispatcher> functionFanoutDispatcherProvider
    ) throws ClassNotFoundException {

        for (EventComponentEntry entry : config.getComponents()) {
            if (entry.isDisabled()) continue;
            Object bean = context.getBean(Class.forName(entry.getHandlerClass()));

            switch (entry.getHandlerType()) {
                case CONSUMER -> {
                    ConsumerDispatcher cd = consumerDispatcherProvider.getIfAvailable();
                    if (cd != null) cd.register(entry.getReadTopics(), (PayloadConsumer) bean);
                }
                case SUPPLIER -> {
                    SupplierDispatcher sd = supplierDispatcherProvider.getIfAvailable();
                    if (sd != null) sd.register((PayloadSupplier) bean);
                }
                case SUPPLIER_FANOUT -> {
                    SupplierFanoutDispatcher sfd = supplierFanoutDispatcherProvider.getIfAvailable();
                    if (sfd != null) sfd.register((PayloadSupplierFanout) bean);
                }
                case FUNCTION -> {
                    FunctionDispatcher fd = functionDispatcherProvider.getIfAvailable();
                    if (fd != null) fd.register(entry.getReadTopics(), (PayloadFunction) bean);
                }
                case FUNCTION_FANOUT -> {
                    FunctionFanoutDispatcher ffd = functionFanoutDispatcherProvider.getIfAvailable();
                    if (ffd != null) ffd.register(entry.getReadTopics(), (PayloadFunctionFanout) bean);
                }
                default -> throw new IllegalArgumentException("Unknown type: " + entry.getHandlerType());
            }
        }
    }
}
