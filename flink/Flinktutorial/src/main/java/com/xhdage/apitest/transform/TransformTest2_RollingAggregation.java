package com.xhdage.apitest.transform;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


// 滚动算子
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取数据
        DataStream<String> dataStream = env.readTextFile("D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        // 转换为SensorReading类型
//        DataStream<SensorReading> dataStream1 = dataStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        });

        // 使用lambda表达式
        DataStream<SensorReading> dataStream1 = dataStream.map( line -> {
                String[] fields = line.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> id = dataStream1.keyBy("id");
        // lambda表达式
        // KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream1.keyBy(data -> data.getId());
        // 方法引用，替换上面一行，可读性不好
        // KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream1.keyBy(SensorReading::getId);

        // 滚动聚合
        DataStream<SensorReading> resultStream = id.maxBy("temperature");


        resultStream.print();

        env.execute();
    }
}
