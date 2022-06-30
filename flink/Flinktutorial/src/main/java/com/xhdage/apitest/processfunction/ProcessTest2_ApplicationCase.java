package com.xhdage.apitest.processfunction;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// 应用实例，检查温度在一定时间内连续上升
public class ProcessTest2_ApplicationCase {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("10.22.3.50", 6666);

        DataStream<SensorReading> dataStream = stringDataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy("id")
                .process( new TempIncreaseWaring(10L))
                .print();


        env.execute();
    }


    // 实现自定义的处理函数
    public static class TempIncreaseWaring extends KeyedProcessFunction<Tuple, SensorReading, String>{

        // 控制多长时间
        private Long interval;

        // 保存上一次的温度
        private ValueState<Double> lastTemp;

        // 保存定时器时间
        private ValueState<Long> timeState;

        public TempIncreaseWaring(Long interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last_time", Double.class));
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time_state", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            if(lastTemp.value() != null){
                if(lastTemp.value() < value.getTemperature() && timeState.value()  == null){
                    // 开始计时
                    timeState.update(ctx.timerService().currentProcessingTime() + interval * 1000L);
                    ctx.timerService().registerProcessingTimeTimer(timeState.value());
                }
                else if (lastTemp.value() >= value.getTemperature() && timeState.value() != null){
                    // 删除计时，重新开始，考虑到第一次前后两个温度相等或者前者大于后者时，没有定时器
                    if(timeState.value() != null) {
                        ctx.timerService().deleteProcessingTimeTimer(timeState.value());
                        timeState.clear();
                    }
                }
            }
            lastTemp.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器" + ctx.getCurrentKey() + "温度值连续" + interval + "s上升");
            timeState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTemp.clear();
            timeState.clear();
        }
    }

    // 如果存在连续10s内温度持续上升的情况，则报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<String, SensorReading, String> {

        public TempConsIncreWarning(Long interval) {
            this.interval = interval;
        }

        // 报警的时间间隔(如果在interval时间内温度持续上升，则报警)
        private Long interval;

        // 上一个温度值
        private ValueState<Double> lastTemperature;
        // 最近一次定时器的触发时间(报警时间)
        private ValueState<Long> recentTimerTimeStamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemperature", Double.class));
            recentTimerTimeStamp = getRuntimeContext().getState(new ValueStateDescriptor<Long>("recentTimerTimeStamp", Long.class));
        }

        @Override
        public void close() throws Exception {
            lastTemperature.clear();
            recentTimerTimeStamp.clear();
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 当前温度值
            double curTemp = value.getTemperature();
            // 上一次温度(没有则设置为当前温度)
            double lastTemp = lastTemperature.value() != null ? lastTemperature.value() : curTemp;
            // 计时器状态值(时间戳)
            Long timerTimestamp = recentTimerTimeStamp.value();

            // 如果 当前温度 > 上次温度 并且 没有设置报警计时器，则设置
            if (curTemp > lastTemp && null == timerTimestamp) {
                long warningTimestamp = ctx.timerService().currentProcessingTime() + interval;
                ctx.timerService().registerProcessingTimeTimer(warningTimestamp);
                recentTimerTimeStamp.update(warningTimestamp);
            }
            // 如果 当前温度 < 上次温度，且 设置了报警计时器，则清空计时器
            else if (curTemp <= lastTemp && timerTimestamp != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTimestamp);
                recentTimerTimeStamp.clear();
            }
            // 更新保存的温度值
            lastTemperature.update(curTemp);
        }

        // 定时器任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 触发报警，并且清除 定时器状态值
            out.collect("传感器" + ctx.getCurrentKey() + "温度值连续" + interval + "ms上升");
            recentTimerTimeStamp.clear();
        }
    }
}
