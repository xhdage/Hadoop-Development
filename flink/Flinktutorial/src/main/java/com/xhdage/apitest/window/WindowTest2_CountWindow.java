package com.xhdage.apitest.window;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        // DataStream<String> inputStream = env.readTextFile("/tmp/Flink_Tutorial/src/main/resources/sensor.txt");
        DataStream<String> inputStream = env.socketTextStream("10.22.3.50", 6666);


        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开计数窗口测试

        SingleOutputStreamOperator<Double> aggregateDataStream = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> doubleIntegerTuple2) {
                        return new Tuple2<>(doubleIntegerTuple2.f0 + sensorReading.getTemperature(), doubleIntegerTuple2.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
                        return doubleIntegerTuple2.f0  / doubleIntegerTuple2.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
                        return new Tuple2<>(doubleIntegerTuple2.f0 + acc1.f0, doubleIntegerTuple2.f1 + acc1.f1);
                    }
                });

        // 其他API
        OutputTag<SensorReading> late = new OutputTag<>("late");

        SingleOutputStreamOperator<SensorReading> sum = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                       .trigger()   // 触发器
//                       .evictor()   // 过滤器
                .allowedLateness(Time.minutes(1))   // 处理迟到数据，建议时间设置小些
                .sideOutputLateData(late)  // 测输出流
                .sum("temperature");
        sum.getSideOutput(late).print("late");


        aggregateDataStream.print();

        env.execute();
    }
}
