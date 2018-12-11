package com.buddyjack.flink.sink.influxdb;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a InfluxDB database Point.
 */
@Data
public class InfluxDBPoint {

    private String measurement;
    private long timestamp;
    private Map<String, String> tags;
    private Map<String, Object> fields;

    public InfluxDBPoint(String measurement, long timestamp) {
        this.measurement = measurement;
        this.timestamp = timestamp;
        this.fields = new HashMap<>();
        this.tags = new HashMap<>();
    }

    public InfluxDBPoint(String measurement, long timestamp, Map<String, String> tags, Map<String, Object> fields) {
        this.measurement = measurement;
        this.timestamp = timestamp;
        this.tags = tags;
        this.fields = fields;
    }

}
