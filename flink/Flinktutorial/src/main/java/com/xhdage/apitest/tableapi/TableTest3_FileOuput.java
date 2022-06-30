package com.xhdage.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;

public class TableTest3_FileOuput {
    public static void main(String[] args) throws Exception{
        // 创建flink流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建Table API执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 连接文件系统读取文件并进行转换
        String filePath = "D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\sensor.txt";
        tableEnvironment.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                        .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestamp", DataTypes.BIGINT())
                                .field("temp", DataTypes.DOUBLE())
                        )
                                .createTemporaryTable("inputTable");
        // 获取Table API
        Table inputTable = tableEnvironment.from("inputTable");

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
                .filter("id === 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        tableEnvironment.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnvironment.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 4.输出到文件
        // 连接到外部文件注册输出表
        String outfilePath = "D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\out.txt";
        tableEnvironment.connect(new FileSystem().path(outfilePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        // .field("cnt", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        // 执行
        env.execute();


    }
}
