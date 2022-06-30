package com.xhdage.apitest.processfunction;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("10.22.3.50", 6666);

        DataStream<SensorReading> dataStream = stringDataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 定义一个OutputTag，用来表示测输出流

        OutputTag<SensorReading> lowTemp = new OutputTag<SensorReading>("lowTemp"){};

        // 测试KeyedProcessFunction，自定义测输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                // 判斷溫度大于30輸出到高溫劉
                if (value.getTemperature() >= 30) {
                    out.collect(value);
                } else {
                    ctx.output(lowTemp, value);
                }
            }
        });
        
        highTempStream.print("high_temp");
        highTempStream.getSideOutput(lowTemp).print("low_temp");

        env.execute();
    }
}

