package com.pulsar.common.common_kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

public abstract class AbstractKafkaListener<T> {

    public abstract void processMessage(T payload);

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, T> record){
        processMessage(record.value());
    }
}
