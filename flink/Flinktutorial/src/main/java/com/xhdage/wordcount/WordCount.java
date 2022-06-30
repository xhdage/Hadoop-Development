package com.xhdage.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理文件
 */

public class WordCount {
    public static void main(String[] args) throws Exception {

        // 建立运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String filePath = "D:\\开发\\Java\\bigdata\\flink\\Flinktutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> stringDataSet = env.readTextFile(filePath);

        // 对数据集进行处理，按照空格来分词,转换为(word.1)来进行统计
        DataSet<Tuple2<String, Integer>> sum = stringDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        sum.print();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            // 遍历所有word，包成二元组输出
            for (String str :words){
                collector.collect(new Tuple2<>(str, 1));
            }

        }
    }
}
