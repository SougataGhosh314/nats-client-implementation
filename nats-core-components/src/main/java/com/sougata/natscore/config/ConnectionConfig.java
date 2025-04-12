package com.sougata.natscore.config;

import io.nats.client.Connection;
import io.nats.client.Nats;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
@ConfigurationProperties(prefix = "nats")
@Getter
@Setter
public class ConnectionConfig {
    private String url;

    @Bean(destroyMethod = "close")
    public Connection connection() throws IOException, InterruptedException {
        return Nats.connect(url);
    }
}
