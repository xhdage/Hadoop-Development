package com.xhdage.apitest.sink;

import com.xhdage.apitest.beans.SensorReading;
import com.xhdage.apitest.source.SourceTest4_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

// 自定义Sink To JDBC
public class SinkTest4_Jdbc {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("/tmp/Flink_Tutorial/src/main/resources/sensor.txt");
//
//        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//        });

        // 自定义数据源生成数据
        DataStream<SensorReading> dataStream = env.addSource(new SourceTest4_UDF.MySensorSource());


        dataStream.addSink(new MyJdbcSink());

        env.execute();
    }

    // 自定义Sink到mysql
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        private Connection connection;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setString(1, value.getId());
            updateStmt.setDouble(2, value.getTemperature());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0){
                insertStmt.setDouble(1, value.getTemperature());
                insertStmt.setString(2, value.getId());
                insertStmt.execute();
            }
        }

        public MyJdbcSink() {
            super();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_test?useUnicode=true&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&useSSL=false"
                    , "root", "123456");
            insertStmt = connection.prepareStatement("update flink_test set temp = ?  where id = ?");
            updateStmt = connection.prepareStatement("insert into flink_test(id, temp) values (?, ?");

        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
