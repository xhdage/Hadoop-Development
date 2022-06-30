package com.xhdage.apitest.sink;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import redis.clients.jedis.Jedis;

public class SinkTest2_RedisSink {
    public static void main(String[] args) throws Exception{
        
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取源文件
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\sensor.txt");
        
        // 转换为SensorReading
        DataStream<SensorReading> sensorReadingDataStream = dataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        // 定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("10.22.3.50")
                .setPort(6379)
                .setPassword("123456")
                .setDatabase(0)
                .build();


        // 将数据输出到redis
        sensorReadingDataStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        // 执行
        env.execute();

    }

    // 自定义RedisMapper
    public static class MyRedisMapper implements RedisMapper<SensorReading>{

        // 定义保存数据到redis的命令，存成hash表，hset sensor_temp, id temperature
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(SensorReading data) {
            return data.getTemperature().toString();
        }
    }
}
