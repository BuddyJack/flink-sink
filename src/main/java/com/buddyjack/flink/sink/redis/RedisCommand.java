package com.buddyjack.flink.sink.redis;


import lombok.Data;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

@Data
public abstract class RedisCommand implements Serializable {
    String key;
    Object value;
    int expire;

    public RedisCommand(String key, Object value, int expire) {
        this.key = key;
        this.value = value;
        this.expire = expire;
    }


    public RedisCommand(String key, Object value) {
        this.key = key;
        this.value = value;
        this.expire = -1;
    }

    public void execute(Jedis jedis) {
        invokeByCommand(jedis);
        if (-1 < this.expire) {
            jedis.expire(key, expire);
        }
    }

    public abstract void invokeByCommand(Jedis jedis);

}
