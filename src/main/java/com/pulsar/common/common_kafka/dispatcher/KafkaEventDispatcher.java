package com.pulsar.common.common_kafka.dispatcher;

import com.pulsar.common.common_kafka.dto.KafkaEvent;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaEventDispatcher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaEventDispatcher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public <T> void send(String topic, KafkaEvent<T> event) {
        kafkaTemplate.send(topic, event);
    }
}
