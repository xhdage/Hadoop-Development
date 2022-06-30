package com.xhdage.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest1_Base {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取数据
        DataStream<String> dataStream = env.readTextFile("D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        // map操作
        DataStream<Integer> dataStream1 = dataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        // Flatmap操作
        DataStream<String> dataStream2 = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields){
                    collector.collect(field);
                }
            }
        });

        // Filter
        DataStream<String> dataStream3 = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });


        dataStream1.print();
        dataStream2.print();
        dataStream3.print();

        env.execute();
    }
}
