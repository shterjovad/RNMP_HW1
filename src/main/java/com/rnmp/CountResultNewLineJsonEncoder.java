package com.rnmp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rnmp.kafka_data.CountResult;
import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class CountResultNewLineJsonEncoder implements Encoder<CountResult> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void encode(CountResult element, OutputStream stream) throws IOException {
        String jsonStr = objectMapper.writeValueAsString(element);
        stream.write(jsonStr.getBytes(StandardCharsets.UTF_8));
        stream.write('\n');
    }
}
