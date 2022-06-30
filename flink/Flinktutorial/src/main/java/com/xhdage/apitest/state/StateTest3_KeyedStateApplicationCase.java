package com.xhdage.apitest.state;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

public class StateTest3_KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("10.22.3.50", 6666);

        DataStream<SensorReading> dateStream = dataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dateStream.keyBy("id").flatMap(new MyTempatureWarning(10.0));

        resultStream.print();
        env.execute();
    }

    public static class MyTempatureWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>>{
        // 定义报警温度阈值
        private final Double tempWarningThreshold;
        // 定义状态保存上次温度值
        private ValueState<Double> lastTemperature;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void close() throws Exception {
            // 手动释放资源
            lastTemperature.clear();
        }

        public MyTempatureWarning(Double tempWarningThreshold) {
            this.tempWarningThreshold = tempWarningThreshold;
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            Double lastTemp = lastTemperature.value();
            Double nowTemp = sensorReading.getTemperature();

            if (lastTemp != null){
                if(Math.abs(lastTemp - nowTemp) >= tempWarningThreshold){
                    collector.collect(new Tuple3<>(sensorReading.getId(), lastTemp, nowTemp));
                }
            }

            lastTemperature.update(nowTemp);

        }
    }
}
