package com.xhdage.apitest.tableapi.udf;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;


// 标量函数
public class UdfTest1_ScalaFunction {
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


        // 6、自定义标量函数，实现求id的HashCode
        HashCode hashCode = new HashCode(23);
        // 需要在环境中注册udf
        tableEnvironment.registerFunction("hashCode", hashCode);

        // 6.1 Table Api
        Table resultTable = sensorTable.select("id, ts, hashCode(id)");

        // 6.2 SQL，需要提前注册表
        tableEnvironment.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnvironment.sqlQuery("select id, ts, hashCode(id) from sensor");

        tableEnvironment.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnvironment.toAppendStream(resultSqlTable, Row.class).print("resultSqlTable");

        env.execute();
    }

    public static class HashCode extends ScalarFunction{

        private int factor = 13;

        public HashCode(int factor){
            this.factor = factor;
        }

        public int eval(String id){
            return id.hashCode() + factor;
        }
    }
}
