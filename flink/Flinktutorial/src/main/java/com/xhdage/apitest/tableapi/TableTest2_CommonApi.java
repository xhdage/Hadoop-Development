package com.xhdage.apitest.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception{
        // 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 1.1 基于老版本planner的流式处理
//        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();
//
//        StreamTableEnvironment oldTableEnvironment = StreamTableEnvironment.create(env, oldStreamSettings);

        // 1.2 基于老版本的planner的批处理,使用的是DataSet，不像新版的blank,流和批都用的DataStream
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 1.3 基于Blink的流处理
//        EnvironmentSettings blankStreamSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment blinkTableEnvironment = StreamTableEnvironment.create(env, blankStreamSettings);

        // 1.4 基于Blink的批处理
//        EnvironmentSettings blankBatchSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blankBatchSettings);


        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\sensor.txt";
        tableEnvironment.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT() )
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");
        Table inputTable = tableEnvironment.from("inputTable");
        // inputTable.printSchema();
        // tableEnvironment.toAppendStream(inputTable, Row.class).print();

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
                        .filter("id === 'sensor_6'");

        // 查看表的执行计划
        String explaination = tableEnvironment.explain(resultTable);
        System.out.println(explaination);

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        tableEnvironment.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnvironment.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 打印输出
        // tableEnvironment.toAppendStream(resultTable, Row.class).print("result");
        // tableEnvironment.toRetractStream(aggTable, Row.class).print("agg");
        // tableEnvironment.toRetractStream(sqlAggTable, Row.class).print("sqlAgg");

        env.execute();

    }
}
