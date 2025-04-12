package com.sougata.nats.config;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class NatsGatewayConfig {

    @Bean
    public Connection gatewayNatsConnection() throws IOException, InterruptedException {
        Options options = new Options.Builder()
                .server("nats://localhost:4222")
                .connectionName("api-gateway")
                .build();

        return Nats.connect(options);
    }
}
