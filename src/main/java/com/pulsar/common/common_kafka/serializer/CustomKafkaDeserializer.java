package com.pulsar.common.common_kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomKafkaDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> targetType;

    public CustomKafkaDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null) return null;

        try {
            return objectMapper.readValue(bytes, targetType);
        } catch (Exception e) {
            throw new SerializationException("error: ", e);
        }
    }
}
