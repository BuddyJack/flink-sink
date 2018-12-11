package com.buddyjack.flink.sink.influxdb;

import lombok.Data;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for InfluxDB.
 */
@Data
public class InfluxDBConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_ACTIONS = 2000;
    private static final int DEFAULT_FLUSH_DURATION = 100;

    private String url;
    private String username;
    private String password;
    private String database;
    private int batchActions;
    private int flushDuration;
    private TimeUnit flushDurationTimeUnit;
    private boolean enableGzip;

    public InfluxDBConfig(InfluxDBConfig.Builder builder) {
        Preconditions.checkArgument(builder != null, "InfluxDBConfig builder can not be null");

        this.url = Preconditions.checkNotNull(builder.getUrl(), "host can not be null");
        this.username = Preconditions.checkNotNull(builder.getUsername(), "username can not be null");
        this.password = Preconditions.checkNotNull(builder.getPassword(), "password can not be null");
        this.database = Preconditions.checkNotNull(builder.getDatabase(), "database name can not be null");

        this.batchActions = builder.getBatchActions();
        this.flushDuration = builder.getFlushDuration();
        this.flushDurationTimeUnit = builder.getFlushDurationTimeUnit();
        this.enableGzip = builder.isEnableGzip();
    }


    public static Builder builder(String url, String username, String password, String database) {
        return new Builder(url, username, password, database);
    }

    @Data
    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String database;
        private int batchActions = DEFAULT_BATCH_ACTIONS;
        private int flushDuration = DEFAULT_FLUSH_DURATION;
        private TimeUnit flushDurationTimeUnit = TimeUnit.MILLISECONDS;
        private boolean enableGzip = false;


        public Builder(String url, String username, String password, String database) {
            this.url = url;
            this.username = username;
            this.password = password;
            this.database = database;
        }


        public InfluxDBConfig.Builder url(String url) {
            this.url = url;
            return this;
        }

        public InfluxDBConfig.Builder username(String username) {
            this.username = username;
            return this;
        }

        public InfluxDBConfig.Builder password(String password) {
            this.password = password;
            return this;
        }

        public InfluxDBConfig.Builder database(String database) {
            this.database = database;
            return this;
        }


        public InfluxDBConfig.Builder batchActions(int batchActions) {
            this.batchActions = batchActions;
            return this;
        }


        public Builder flushDuration(int flushDuration, TimeUnit flushDurationTimeUnit) {
            this.flushDuration = flushDuration;
            this.flushDurationTimeUnit = flushDurationTimeUnit;
            return this;
        }

        public InfluxDBConfig.Builder enableGzip(boolean enableGzip) {
            this.enableGzip = enableGzip;
            return this;
        }


        public InfluxDBConfig build() {
            return new InfluxDBConfig(this);
        }
        
    }
}
