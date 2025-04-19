package com.sougata.natscore.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sougata.natscore.enums.HandlerType;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@Getter
public class EventComponentConfigLoader {

    @Bean
    public EventComponentConfig eventComponentConfig() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("event-config.json")) {
            if (is == null) {
                throw new FileNotFoundException("event-config.json not found in classpath");
            }

            EventComponentConfig config = mapper.readValue(is, EventComponentConfig.class);
            validateReadTopicBindings(config);
            validateReadWriteConflicts(config);
            log.info("event-config validation is successful.");
            return config;
        }
    }

    public void validateReadTopicBindings(EventComponentConfig config) {
        log.info("Validating read topic bindings...");
        Map<String, Map<HandlerType, Set<String>>> topicHandlerMap = new HashMap<>();

        for (EventComponentEntry entry : config.getComponents()) {
            if (entry.isDisabled() || entry.getReadTopics() == null) continue;

            for (TopicBinding binding : entry.getReadTopics()) {
                String topic = binding.getTopicName();
                String queueGroup = binding.getQueueGroup(); // can be null
                HandlerType type = entry.getHandlerType();

                topicHandlerMap
                        .computeIfAbsent(topic, t -> new HashMap<>())
                        .computeIfAbsent(type, t -> new HashSet<>())
                        .add(queueGroup);
            }
        }

        for (Map.Entry<String, Map<HandlerType, Set<String>>> entry : topicHandlerMap.entrySet()) {
            String topic = entry.getKey();
            Map<HandlerType, Set<String>> handlersOnTopic = entry.getValue();

            // ❌ Rule: Different handler types on the same topic
            if (handlersOnTopic.size() > 1) {
                throw new IllegalStateException("Topic [" + topic + "] has multiple handler types: " + handlersOnTopic.keySet());
            }

            // ✅ Safe to validate queue group uniqueness
            Set<String> queueGroups = handlersOnTopic.values().iterator().next();
            List<String> nonNullGroups = new ArrayList<>();

            for (String q : queueGroups) {
                if (q == null) {
                    nonNullGroups.add("null");
                } else if (nonNullGroups.contains(q)) {
                    throw new IllegalStateException("Duplicate queue group [" + q + "] on topic [" + topic + "]");
                } else {
                    nonNullGroups.add(q);
                }
            }

            // ❌ Rule: more than one handler with null queueGroup
            long nullCount = queueGroups.stream().filter(Objects::isNull).count();
            if (nullCount > 1) {
                throw new IllegalStateException("Topic [" + topic + "] has multiple handlers without queue groups (null)");
            }
        }
    }

    private void validateReadWriteConflicts(EventComponentConfig config) {
        log.info("Validating read-write topic conflicts...");
        Set<String> writtenTopics = config.getComponents().stream()
                .filter(c -> !c.isDisabled())
                .flatMap(c -> Optional.ofNullable(c.getWriteTopics()).stream().flatMap(Collection::stream))
                .map(TopicBinding::getTopicName)
                .collect(Collectors.toSet());

        Set<String> readTopics = config.getComponents().stream()
                .filter(c -> !c.isDisabled())
                .flatMap(c -> Optional.ofNullable(c.getReadTopics()).stream().flatMap(Collection::stream))
                .map(TopicBinding::getTopicName)
                .collect(Collectors.toSet());

        Set<String> overlap = new HashSet<>(writtenTopics);
        overlap.retainAll(readTopics);

        if (!overlap.isEmpty()) {
            throw new IllegalStateException(
                    "Invalid config: Topics used for both publishing and subscribing: " + String.join(", ", overlap)
            );
        }
    }
}
