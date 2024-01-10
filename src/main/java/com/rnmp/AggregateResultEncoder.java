package com.rnmp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rnmp.kafka_data.AggregateResult;
import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class AggregateResultEncoder implements Encoder<AggregateResult> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void encode(AggregateResult element, OutputStream stream) throws IOException {
        String jsonStr = objectMapper.writeValueAsString(element);
        stream.write(jsonStr.getBytes(StandardCharsets.UTF_8));
        stream.write('\n');
    }
}
