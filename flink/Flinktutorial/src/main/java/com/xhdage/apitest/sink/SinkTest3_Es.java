package com.xhdage.apitest.sink;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SinkTest3_Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/tmp/Flink_Tutorial/src/main/resources/sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义es的连接配置
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());

        env.execute();
    }

    // 实现自定义的ES写入操作
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {
        @Override
        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
            // 定义写入的数据source
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", element.getId());
            dataSource.put("temp", element.getTemperature().toString());
            dataSource.put("ts", element.getTimestamp().toString());

            // 创建请求，作为向es发起的写入命令(ES7统一type就是_doc，不再允许指定type)
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .source(dataSource);

            // 用index发送请求
            indexer.add(indexRequest);
        }
    }
}