package com.sougata.natscore.config;

import lombok.Data;

import java.util.List;

@Data
public class EventComponentEntry {
    private String type; // function, consumer, supplier
    private List<TopicBinding> readTopics;
    private List<TopicBinding> writeTopics;
    private String handlerClass;
}
