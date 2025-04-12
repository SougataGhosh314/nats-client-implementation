package com.sougata.natscore.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;

@Configuration
public class EventComponentConfigLoader {

    @Bean
    public EventComponentConfig eventComponentConfig() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        InputStream is = getClass().getClassLoader().getResourceAsStream("event-config.json");
        return mapper.readValue(is, EventComponentConfig.class);
    }
}
