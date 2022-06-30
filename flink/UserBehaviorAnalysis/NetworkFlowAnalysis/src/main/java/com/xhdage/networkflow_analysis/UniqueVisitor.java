package com.xhdage.networkflow_analysis;

import com.xhdage.networkflow_analysis.bean.PageViewCount;
import com.xhdage.networkflow_analysis.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.net.URL;
import java.util.HashSet;

public class UniqueVisitor {
    public static void main(String[] args) throws Exception{
        // 1、建立流式运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
        // 2、读取数据
        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputStream = environment.readTextFile(resource.getPath());
        
        // 3、转化为POJO类型并设置时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> userBehaviorDataStream  = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 4、开窗统计UV
        SingleOutputStreamOperator<PageViewCount> uvStream = userBehaviorDataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());

        uvStream.print();

        environment.execute();


    }

    // 实现自定义全窗口函数
    public static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, java.lang.Iterable<UserBehavior> values, Collector<PageViewCount> out) throws Exception {
            // 定义一个Set结构，保存窗口中的所有userId，自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for (UserBehavior ub : values) {
                uidSet.add(ub.getUserId());
            }
            out.collect(new PageViewCount("uv", window.getEnd(), (long) uidSet.size()));
        }
    }
}
