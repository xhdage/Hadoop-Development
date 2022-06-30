package com.xhdage.apitest.transform;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// reduce操作
public class TransformTest3_Reduce {

    public static void main(String[] args) throws Exception {
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

        // 使用lambda表达式
//        DataStream<SensorReading> dataStream1 = dataStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyResultStream = dataStream1.keyBy("id");

        // 使用Reduce，取得当前最新时间和最大温度值
        DataStream<SensorReading> reduceStream = keyResultStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return new SensorReading(sensorReading.getId(), t1.getTimestamp(),
                        Math.max(sensorReading.getTemperature(), t1.getTemperature()));
            }
        });

        // 使用Reduce，传入lambda表达式
        DataStream<SensorReading> reduceStream2 = keyResultStream.reduce((stateNow, newState) -> {
            return new SensorReading(stateNow.getId(), newState.getTimestamp(),
                    Math.max(stateNow.getTemperature(), newState.getTemperature()));
        });

        // 输出
        reduceStream.print();

        env.execute();
    }
}
