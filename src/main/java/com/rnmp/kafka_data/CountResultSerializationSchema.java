package com.rnmp.kafka_data;

import com.rnmp.kafka_data.CountResult;
import org.apache.flink.api.common.serialization.SerializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CountResultSerializationSchema implements SerializationSchema<CountResult> {

    private transient ObjectMapper objectMapper;

    @Override
    public byte[] serialize(CountResult element) {
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