package com.xhdage.apitest.state;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTes2_KeyedState {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("10.22.3.50", 6666);

        DataStream<SensorReading> dateStream = dataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        SingleOutputStreamOperator<Integer> resultStream = dateStream.keyBy("id")
                .map(new MyRichMapper());
        resultStream.print();

        env.execute();

    }
    public static class MyRichMapper extends RichMapFunction<SensorReading, Integer>{
        // 初始化保存keyedState的变量
        private ValueState<Integer> keyCountState;

        // 其它类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
            //            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>())
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {

            // 其它状态API调用
            // list state
            for(String str: myListState.get()){
                System.out.println(str);
            }
            myListState.add("hello");
            // map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");
            // reducing state
            //            myReducingState.add(value);

            myMapState.clear();


            Integer count = keyCountState.value();
            count = count==null ? 0 : count;
            count ++;
            keyCountState.update(count);
            return count;
        }
    }
}
