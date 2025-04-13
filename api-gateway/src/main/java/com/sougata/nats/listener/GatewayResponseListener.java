package com.sougata.nats.listener;

import com.sougata.nats.registry.CorrelationRegistry;
import com.sougata.natscore.model.PayloadHeader;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.impl.Headers;
import org.springframework.stereotype.Component;

@Component
public class GatewayResponseListener {
    public GatewayResponseListener(Connection connection, CorrelationRegistry registry) {
        Dispatcher dispatcher = connection.createDispatcher(msg -> {
            Headers headers = msg.getHeaders();
            System.out.println(msg);
            System.out.println(headers);

            if (headers != null) {
                String correlationId = headers.getFirst(PayloadHeader.CORRELATION_ID.getKey());
                if (correlationId != null) {
                    registry.complete(correlationId, msg);
                }
            }
        });
        dispatcher.subscribe("user.response"); // Example, can subscribe to multiple
        dispatcher.subscribe("user.summary");
    }
}
