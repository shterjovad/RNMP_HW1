package com.rnmp.kafka_data;

public class AggregateResult {
    private String key;
    private long window_start;
    private long window_end;
    private long count;
    private double average;
    private double min_value;
    private double max_value;

    public AggregateResult(String key, long window_start, long window_end, long count, double average, double min_value, double max_value) {
        this.key = key;
        this.window_start = window_start;
        this.window_end = window_end;
        this.count = count;
        this.average = average;
        this.min_value = min_value;
        this.max_value = max_value;
    }

    public String getKey() {
        return key;
    }
    public long getWindowStart() {
        return window_start;
    }
    public long getWindowEnd() {
        return window_end;
    }
    public long getCount() {
        return count;
    }
    public double getAverage() {
        return average;
    }
    public double getMinValue() {
        return min_value;
    }
    public double getMaxValue() {
        return max_value;
    }

}
