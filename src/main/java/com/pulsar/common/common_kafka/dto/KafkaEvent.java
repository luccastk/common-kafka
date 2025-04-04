package com.pulsar.common.common_kafka.dto;

import java.time.Instant;

public record KafkaEvent<T>(
        String evenType,
        Instant timeStamp,
        T payload
) {
}
