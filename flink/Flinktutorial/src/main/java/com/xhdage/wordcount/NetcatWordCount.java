package com.xhdage.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class NetcatWordCount {
    public static void main(String[] args) throws Exception {

        //创建流式处理的环境,它会根据当前运行的上下文直接得到正确的结果：如果程序是独立运行的，就返回一个本地执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建本地的执行环境
        // LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        // 创建远程的执行环境
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("10.22.3.50", 6123, "D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\target\\Flinktutorial-1.0-SNAPSHOT.jar");

        // 设置执行的线程数
        // env.setParallelism(1);
        // env.disableOperatorChaining();

        // 从文件中读取数据
        // String filePath = "D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\hello.txt";
        // DataStreamSource<String> stringDataStreamSource = env.readTextFile(filePath);

        // 用parameter tool工具从程序启动参数中获取配置参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        // 从Netcat里面读取数据
        DataStream<String> stringDataStreamSource = env.socketTextStream(hostname, port);

        // 对数据集进行处理，按照空格来分词,转换为(word.1)来进行统计
        DataStream<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1).setParallelism(2);

        sum.print().setParallelism(1);

        // 批处理程序是执行前，把操作定义好，然后每次数据到来就按照指定的步骤处理
        env.execute();
    }
}
