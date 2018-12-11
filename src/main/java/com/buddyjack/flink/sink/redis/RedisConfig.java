package com.buddyjack.flink.sink.redis;

import lombok.Data;

import java.io.Serializable;

@Data
public class RedisConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String host = "127.0.0.1";
    private int port = 6379;
    private int database = 0;
    private String password = null;
    protected int maxTotal = 8;
    protected int maxIdle = 8;
    protected int minIdle = 0;
    protected int connectionTimeout = 2000;

    public RedisConfig host(String host) {
        setHost(host);
        return this;
    }

    public RedisConfig port(int port) {
        setPort(port);
        return this;
    }

    public RedisConfig database(int database) {
        setDatabase(database);
        return this;
    }

    public RedisConfig password(String password) {
        setPassword(password);
        return this;
    }

    public RedisConfig maxTotal(int maxTotal) {
        setMaxTotal(maxTotal);
        return this;
    }

    public RedisConfig maxIdle(int maxIdle) {
        setMaxIdle(maxIdle);
        return this;
    }

    public RedisConfig minIdle(int minIdle) {
        setMinIdle(minIdle);
        return this;
    }

    public RedisConfig connectionTimeout(int connectionTimeout) {
        setConnectionTimeout(connectionTimeout);
        return this;
    }
}
