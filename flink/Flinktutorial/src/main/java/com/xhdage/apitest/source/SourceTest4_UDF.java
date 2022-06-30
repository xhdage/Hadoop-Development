package com.xhdage.apitest.source;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中读取数据
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();

        env.execute();
    }

    // 自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading>{
        // 定义一个标志为，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            // 初始化十个温度值
            Random random =new Random();
            // 保存十个温度的值，为了后续方便更新，采用HashMap
            HashMap<String, Double> sensorTemp = new HashMap<>();

            for (int i = 0 ; i < 10; i++){
                sensorTemp.put("sensor_" + (i + 1), 60 + random.nextGaussian()*20);
            }

            while (running){
                for (String sensorId : sensorTemp.keySet()){
                    // 取得随机值
                    Double sensor = sensorTemp.get(sensorId) + random.nextGaussian();
                    // 更新
                    sensorTemp.put(sensorId, sensor);
                    sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(), sensor));
                }

                Thread.sleep(2000);

            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
