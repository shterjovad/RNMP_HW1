package com.rnmp.kafka_data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;



public class AggregateResultSerializationSchema implements SerializationSchema<AggregateResult> {
    private transient ObjectMapper objectMapper;

    @Override
    public byte[] serialize(AggregateResult element) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize CountResult", e);
        }
    }
}
