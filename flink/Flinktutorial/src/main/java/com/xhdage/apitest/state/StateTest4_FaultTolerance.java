package com.xhdage.apitest.state;

import com.xhdage.apitest.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("checkpointDataUri"));
        // 这个需要另外导入依赖
        env.setStateBackend(new RocksDBStateBackend("checkpointDataUri"));

        // 2. 检查点配置
        env.enableCheckpointing(300);
        // 高级配置
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 会覆盖上面的配置，同时只有一个checkpoint
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        // true不允许从savepoint恢复，false表示从checkpoint和savepoint中最近一次恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 容忍失败多少次，checkpoint,0表示不容忍
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 重启策略的配置
        // 固定延时重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 失败率重启,允许在一定时间内失败的次数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print();
        env.execute();
    }

}
