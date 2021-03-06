package com.xhdage.loginfail_detect;

import com.xhdage.loginfail_detect.bean.LoginEvent;
import com.xhdage.loginfail_detect.bean.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFail.class.getResource("LoginLog.csv");
        DataStream<LoginEvent> loginEventStream = environment.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // ???????????????????????????????????????????????????
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream
                .keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        warningStream.print();

        environment.execute("login fail detect job");

    }

    // ???????????????KeyedProcessFunction
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        // ?????????????????????????????????????????????
        private Integer maxFailTimes;
        // ????????????????????????
        ListState<LoginEvent> loginFailEventListState;
        // ?????????????????????????????????????????????????????????????????????????????????
        ValueState<Long> timerTsState;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs-State", Long.class));
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            // ????????????????????????2?????????????????????????????????ListState??????????????????
            ArrayList<LoginEvent> loginFailEvents = Lists.newArrayList(loginFailEventListState.get().iterator());
            int failTimes = loginFailEvents.size();

            if (failTimes >= maxFailTimes) {
                // ??????????????????????????????????????????????????????
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        loginFailEvents.get(0).getTimestamp(),
                        loginFailEvents.get(failTimes - 1).getTimestamp(),
                        "login fail in 2s for " + failTimes + " times"));
            }

            // ????????????
            loginFailEventListState.clear();
            timerTsState.clear();
        }

        @Override
        public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.Context ctx, Collector<LoginFailWarning> out) throws Exception {
            if("fail".equals(value.getLoginState())) {
                loginFailEventListState.add(value);
                // ????????????????????????????????????
                if (null == timerTsState.value()) {
                    Long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    timerTsState.update(ts);
                }
            }
            else {
                // 2. ?????????????????????????????????????????????????????????????????????
                if (null != timerTsState.value()) {
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                loginFailEventListState.clear();
                timerTsState.clear();
            }
        }
    }
}
