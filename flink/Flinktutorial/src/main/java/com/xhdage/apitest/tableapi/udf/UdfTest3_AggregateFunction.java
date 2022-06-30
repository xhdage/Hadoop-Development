package com.xhdage.apitest.tableapi.udf;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

public class UdfTest3_AggregateFunction {
    public static void main(String[] args) throws Exception {
        // 1、创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、读取数据
        String filePath = "D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\sensor.txt";
        DataStreamSource<String> stringDataStreamSource = env.readTextFile(filePath);

        // 3、创建Table执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 4、转换为POJO类
        SingleOutputStreamOperator<SensorReading> dataStream = stringDataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));

        });

        // 5、将数据流转换为表
        Table sensorTable = tableEnvironment.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");


        // 6、自定义聚合函数，求当前传感器的平均温度值
        AvgTemp avgTemp =new AvgTemp();
        // 需要在环境中注册udf
        tableEnvironment.registerFunction("avgTemp",avgTemp);

        // 6.1 Table Api
        Table resultTable = sensorTable
                .groupBy("id")
                        .aggregate("avgTemp(temp) as avgtemp")
                                .select("id , avgtemp");

        // 6.2 SQL，需要提前注册表
        tableEnvironment.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnvironment.sqlQuery("select id, avgTemp(temp) " +
                "from sensor group by id");

        tableEnvironment.toRetractStream(resultTable, Row.class).print("resultTable");
        tableEnvironment.toRetractStream(resultSqlTable, Row.class).print("resultSqlTable");

        env.execute();
    }

    // 自定义的AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>>{

        @Override
        public Double getValue(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 必须实现一个accumulate方法，来数据之后更新状态
        public void accumulate(Tuple2<Double, Integer> acc, Double temp){
            acc.f0 += temp;
            acc.f1 += 1;

        }
    }

}
