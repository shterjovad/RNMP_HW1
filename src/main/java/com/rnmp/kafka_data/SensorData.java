package com.rnmp.kafka_data;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SensorData {
    private String key;
    private int value;
    private long timestamp;

    public SensorData(@JsonProperty("key") String key,
                      @JsonProperty("value") int value,
                      @JsonProperty("timestamp") long timestamp) {
        // Constructor implementation
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    // Getters and setters for key, value, and timestamp

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
