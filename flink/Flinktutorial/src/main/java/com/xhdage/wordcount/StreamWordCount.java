package com.xhdage.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流式处理文件
 */

public class StreamWordCount {
    public static void main(String[] args) throws Exception {

        //创建流式处理的环境
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行的线程数
        // envStream.setParallelism(1);

        // 从文件中读取数据
        String filePath = "D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> stringDataStreamSource = envStream.readTextFile(filePath);

        // 对数据集进行处理，按照空格来分词,转换为(word.1)来进行统计
        DataStream<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);



        sum.print();

        // 批处理程序是执行前，把操作定义好，然后每次数据到来就按照指定的步骤处理
        envStream.execute();
    }
}
