package com.sougata.natscore.config;

import com.sougata.natscore.util.NatsSslUtils;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

@Configuration
@ConfigurationProperties(prefix = "nats")
@Getter
@Setter
public class ConnectionConfig {
    private String url;
    private String credsFile;

    @Bean(destroyMethod = "close")
    public Connection connection() throws Exception {
        Options options = new Options.Builder()
                .server(url) // Or whatever secure URL your server runs on
                .authHandler(Nats.credentials(credsFile))
                .sslContext(NatsSslUtils.createSslContext(
                        "C:/nats-config/certs/ca.pem",
                        null, // No client cert needed for one-way TLS
                        null  // No client key needed for one-way TLS
                ))
                .connectionTimeout(Duration.ofSeconds(5))
                .build();

        return Nats.connect(options);
    }
}
