package com.rnmp.kafka_data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SensorDataDeserializer implements DeserializationSchema<SensorData> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SensorData deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, SensorData.class);
    }

    @Override
    public boolean isEndOfStream(SensorData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<SensorData> getProducedType() {
        return TypeInformation.of(SensorData.class);
    }

}
