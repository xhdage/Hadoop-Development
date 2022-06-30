package com.xhdage.apitest.tableapi;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest1_Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1、读取数据
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("10.22.3.50", 6666);

        // 2、转换为POJO
        DataStream<SensorReading> dataStream = stringDataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3、创建表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 4、基于数据流创建一张表
        Table dataTable = tableEnvironment.fromDataStream(dataStream);

        // 5、调用table API进行转换操作
        Table resultTable = dataTable.select("id, temperature")
                .where("id = 'sensor_1'");

        // 6、执行SQL，得注册表
        tableEnvironment.createTemporaryView("sensor", dataTable);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnvironment.sqlQuery(sql);

        tableEnvironment.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnvironment.toAppendStream(resultSqlTable, Row.class).print("resultSqlTable");

        env.execute();
    }
}
