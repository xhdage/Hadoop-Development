package com.xhdage.apitest.transform;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

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

        DataStream<Tuple2<String, Integer>> dataStream2 = dataStream1.map(new MyMapper2());

        dataStream2.print();

        env.execute();
    }

    public static class MyMapper implements MapFunction<SensorReading, Tuple2<String, Integer>>{

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), sensorReading.getId().length());
        }
    }

    // rich函数，可以得到运行上下文信息
    public static class MyMapper2 extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态或者连接数据库等
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般做一些收尾工作，清空状态或者关闭连接
            System.out.println("close");
        }
    }

}
