package com.buddyjack.flink.sink;

import com.buddyjack.flink.sink.redis.RedisCommand;
import com.buddyjack.flink.sink.redis.RedisConfig;
import com.buddyjack.flink.sink.redis.RedisSink;
import com.buddyjack.flink.sink.redis.command.RedisRPushCommand;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import java.security.MessageDigest;

import java.util.Properties;

public class RedisSinkTest {

    public final static String SOURCE_KAFKA_HOSTS = "192.168.103.34:9092";
    public static final String SOURCE_TOPIC = "yoho_log_mobile";

    public static final String HISTORY_REDIS_HOST = "192.168.102.76";
    public static final int HISTORY_REDIS_PORT = 16379;
    public static final String HISTORY_REDIS_PWD = null;

    public static final int HISTOYR_REDIS_EXPIRE = 1 * 24 * 3600;

    public static void main(String[] args) {
        //历史到redis中
        RedisConfig redisConfig = new RedisConfig().host(HISTORY_REDIS_HOST).port(HISTORY_REDIS_PORT).password(HISTORY_REDIS_PWD);
        RedisSink redisSink = new RedisSink(redisConfig);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //从点击流获取消息
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", SOURCE_KAFKA_HOSTS);
        consumerProperties.put("group.id", "clickstream_clean2_rec");
        consumerProperties.put("auto.offset.reset", "latest");
        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010(SOURCE_TOPIC, new SimpleStringSchema(), consumerProperties);


        //<op,msg>
        DataStream<Tuple2<String, String>> inputStream = env.addSource(consumer010).flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                try {
                    Preconditions.checkArgument(StringUtils.isNotBlank(value));
                    out.collect(Tuple2.of(new String(MessageDigest.getInstance("MD5").digest(value.getBytes())), value));
                } catch (Exception e) {
                    //异常吞掉
                }
            }
        });


        //历史数据存redis
        inputStream.flatMap(new FlatMapFunction<Tuple2<String, String>, RedisCommand>() {
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<RedisCommand> out) throws Exception {
                String[] prdIds = StringUtils.split(value.f1, ",");
                out.collect(new RedisRPushCommand(value.f0, prdIds, HISTOYR_REDIS_EXPIRE));
            }
        }).addSink(redisSink);


    }
}
