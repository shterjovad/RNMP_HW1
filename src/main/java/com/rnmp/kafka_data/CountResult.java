package com.rnmp.kafka_data;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class CountResult {
    private String key;
    private long windowSize;
    private long count;

    public CountResult(String key, long windowStart, long windowEnd, long count) {
        this.key = key;
        this.windowSize = windowEnd - windowStart;
        this.count = count;
    }

    public String getKey() {
        return key;
    }
    public long getWindowSize() {
        return windowSize;
    }
    public long getCount() {
        return count;
    }
}
