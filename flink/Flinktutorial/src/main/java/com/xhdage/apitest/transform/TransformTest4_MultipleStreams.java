package com.xhdage.apitest.transform;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

// 将温度分区为高低两个区间，这里会用到split和select, 最后使用connect合，再comap合coflatmap
public class TransformTest4_MultipleStreams {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取数据
        DataStream<String> dataStream = env.readTextFile("D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        // 转换为SensorReading类型
        DataStream<SensorReading> dataStream1 = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        // 分流操作，按照温度值30度分类
        SplitStream<SensorReading> splitStream = dataStream1.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                Double temperature = sensorReading.getTemperature();
                return (temperature > 30 ? Collections.singletonList("high") : Collections.singletonList("low"));
            }
        });

        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");
        DataStream<SensorReading> all = splitStream.select("high", "low");

        // 输出
        // high.print("高温流");
        // low.print("低温流");

        // 合流 connect,将高温流转换为二元组类型，与低温流连接合并之后，输出状态信息
        // 转换为二元组
        //==================== 使用flatmap=================================
        DataStream<Tuple2<String, Double>> highTuple2 = high.flatMap(new FlatMapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public void flatMap(SensorReading sensorReading, Collector<Tuple2<String, Double>> collector) throws Exception {
                collector.collect(new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature()));
            }
        });
        // 合并
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = highTuple2.connect(low);

        DataStream<Object> resultStream = connect.flatMap(new CoFlatMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public void flatMap1(Tuple2<String, Double> stringDoubleTuple2, Collector<Object> collector) throws Exception {
                collector.collect(new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "high temp waring"));
            }

            @Override
            public void flatMap2(SensorReading sensorReading, Collector<Object> collector) throws Exception {
                collector.collect(new Tuple2<>(sensorReading.getId(), "normal"));
            }
        });

        // ==========================使用map====================================
        DataStream<Tuple2<String, Double>> highTuple2_2 = high.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });
        // 合并
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect1 = highTuple2_2.connect(low);
        DataStream<Object> resultStream2 = connect1.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "High temp warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), "Normal");
            }
        });

        // 输出

        // resultStream.print();
        // resultStream2.print();
        // System.out.println(resultStream.getType());
        // System.out.println(resultStream2.getType());


        // union联合相同的流
        DataStream<SensorReading> unionStream = high.union(low, all);
        unionStream.print();


        env.execute();
    }
}
