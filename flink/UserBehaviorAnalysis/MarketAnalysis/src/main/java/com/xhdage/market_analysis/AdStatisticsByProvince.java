package com.xhdage.market_analysis;

import com.xhdage.market_analysis.beans.AdClickEvent;
import com.xhdage.market_analysis.beans.AdCountByProvince;
import com.xhdage.market_analysis.beans.BlackListUserWarning;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.util.Date;

public class AdStatisticsByProvince {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 1、读取数据
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        // DataStreamSource<String> inputStream = environment.readTextFile(resource.getPath());
        DataStream<String> inputStream = environment.readTextFile("D:\\开发\\Java\\bigdata\\flink\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\resources\\AdClickLog.csv");

        DataStream<AdClickEvent> adClickEventDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });
        // 2. 对同一个用户点击同一个广告的行为进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventDataStream
                .keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickEvent value) throws Exception {
                        return new Tuple2<>(value.getUserId(), value.getAdId());
                    }
                })
                .process(new FilterBlackListUser(100));

        // 3、基于省份分组
        SingleOutputStreamOperator<AdCountByProvince> adCountStream = adClickEventDataStream
                .keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountStream.print();

        environment.execute("ad count by province job");
    }

    public static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class AdCountResult implements WindowFunction<Long, AdCountByProvince, String, TimeWindow> {

        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountByProvince(province, windowEnd, count));
        }
    }

    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {

        // 定义属性：点击次数上线
        private Integer countUpperBound;

        // 定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countState;
        // 定义一个标志状态，保存当前用户是否已经被发送到了黑名单里
        ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class));
        }

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 清空所有状态
            countState.clear();
            isSentState.clear();
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            // 判断当前用户对同一广告的点击次数，如果不够上限，该count加1正常输出；
            // 如果到达上限，直接过滤掉，并侧输出流输出黑名单报警

            // 首先获取当前count值
            Long curCount = countState.value();

            Boolean isSent = isSentState.value();

            if (null == curCount) {
                curCount = 0L;
            }

            if (null == isSent) {
                isSent = false;
            }

            // 1. 判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount == 0) {
                long ts = ctx.timerService().currentProcessingTime();
                long fixedTime = DateUtils.addDays(new Date(ts), 1).getTime();
                ctx.timerService().registerProcessingTimeTimer(fixedTime);
            }

            // 2. 判断是否报警
            if (curCount >= countUpperBound) {
                // 判断是否输出到黑名单过，如果没有的话就输出到侧输出流
                if (!isSent) {
                    isSentState.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist") {
                               },
                            new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over " + countUpperBound + "times."));
                }
                // 不再进行下面操作
                return;
            }

            // 如果没有返回，点击次数加1，更新状态，正常输出当前数据到主流
            countState.update(curCount + 1);
            out.collect(value);
        }

    }
}
