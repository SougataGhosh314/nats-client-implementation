package com.sougata.natscore.config;

import lombok.Data;

@Data
public class TopicBinding {
    private String topicName;
    private String messageType; // Fully-qualified class name for the expected message
}