package com.buddyjack.flink.sink;

import com.buddyjack.flink.sink.influxdb.InfluxDBConfig;
import com.buddyjack.flink.sink.influxdb.InfluxDBPoint;
import com.buddyjack.flink.sink.influxdb.InfluxDBSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class InfluxdbSinkTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);
        Table table = tableEnv.scan("");
        DataStream<InfluxDBPoint> influxStream = tableEnv.toAppendStream(table, Row.class).map(new MapFunction<Row, InfluxDBPoint>() {
            @Override
            public InfluxDBPoint map(Row value) throws Exception {
                InfluxDBPoint influxDBPoint = new InfluxDBPoint("nginx_access_count", ((Timestamp) value.getField(0)).getTime() + 8 * 3600 * 1000);
                Map<String, Object> fields = new HashMap<>();
                fields.put("pv", (Long) value.getField(1));
                fields.put("uv", (Long) value.getField(2));
                influxDBPoint.setFields(fields);
                Map<String, String> tags = new HashMap<>();
                tags.put("region", (String) value.getField(3));
                tags.put("service", (String) value.getField(4));
                influxDBPoint.setTags(tags);
                return influxDBPoint;
            }
        });
        InfluxDBConfig influxDBConfig = InfluxDBConfig.builder("http://10.66.80.6:8086", "root", "root", "nginx_access").build();
        influxStream.addSink(new InfluxDBSink(influxDBConfig));
    }
}
