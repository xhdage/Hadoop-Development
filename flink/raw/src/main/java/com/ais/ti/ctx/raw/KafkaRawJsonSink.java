package com.ais.ti.ctx.raw;

import com.ais.ti.ctx.raw.entities.SinkPathData;
import com.ais.ti.ctx.raw.entities.TopicValueSchema;
import com.ais.ti.ctx.raw.func.params.Params;
import com.ais.ti.ctx.raw.func.writer.JsonWriter;
import com.ais.ti.ctx.raw.sink.SimpleBucketingSink;
import com.ais.ti.ctx.raw.entities.TopicValue;
import com.ais.ti.ctx.raw.func.bucketer.MetaPathBucketer;
import com.ais.ti.ctx.raw.func.flatMap.TopicValue2JsonFlatMap;
import com.ais.ti.ctx.raw.utils.HdfsUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 将原数据以json格式存入hdfs。
 */
public class KafkaRawJsonSink {
    public static Properties getKafkaParams(
            String servers, String groupId, String partitionDiscoveryInterval) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "134218752");
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "134217728");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "1048576");
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        properties.setProperty(ConsumerConfig.SEND_BUFFER_CONFIG, "131072");
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "2000");
        // 动态分区发现
        properties.setProperty("flink.partition-discovery.interval-millis", partitionDiscoveryInterval);
        return properties;
    }

    public static void main(String[] args) throws Exception {
        // environment and parameter.
        Params params = new Params(args);
        //        int sourceParallelism = params.getSourceParallelism();
        //        int flatMapParallelism = params.getFlatMapParallelism();
        //        int sinkParallelism = params.getSinkParallelism();
        int envParallelism = params.getEnvParallelism();

        long checkpointInterval = params.getCheckpointInterval();
        String checkpointDir = params.getCheckpointDir();
        int restartAttempts = params.getRestartAttempts();
        int restartInterval = params.getRestartInterval();
        String kafkaServer = params.getKafkaServer(); // required
        String kafkaGroupId = params.getKafkaGroupId(); // required
        String kafkaTopicPattern = params.getKafkaTopicPattern(); // required
        String sinkBasePath = params.getSinkBasePath(); // required
        String jobName = params.getJobName();
        String partitionDiscoveryInterval = params.getPartitionDiscoveryInterval();
        String bucketDateFormat = params.getBucketDateFormat();
        long batchSize = params.getBatchSize();
        long batchRolloverInterval = params.getBatchRolloverInterval();
        long bucketCheckInterval = params.getBucketCheckInterval();
        long inactiveBucketThreshold = params.getInactiveBucketThreshold();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // conf
        env.setParallelism(envParallelism);
        env.enableCheckpointing(checkpointInterval);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        restartAttempts, Time.of(restartInterval, TimeUnit.SECONDS)));
        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // allow job recovery fallback to checkpoint when there is a more recent savepoint
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.setStateBackend(new FsStateBackend(checkpointDir, true));

        Properties kafkaParams = getKafkaParams(kafkaServer, kafkaGroupId, partitionDiscoveryInterval);
        // source
        FlinkKafkaConsumer<TopicValue> consumer =
                new FlinkKafkaConsumer<>(
                        Pattern.compile(kafkaTopicPattern), new TopicValueSchema(), kafkaParams);
        consumer.setStartFromGroupOffsets();
        consumer.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<TopicValue> topicValueDataStreamSource = env.addSource(consumer);

        // etl
        KeyedStream<SinkPathData, String> sinkPathDataStringKeyedStream =
                topicValueDataStreamSource
                        .flatMap(new TopicValue2JsonFlatMap())
                        .keyBy(SinkPathData::getPath); // 根据path分区

        // sink
        //        bucketDateFormat = "yyyy-MM-dd--HH-mm";
        SimpleBucketingSink<SinkPathData> jsonHdfsSink =
                new SimpleBucketingSink<>(sinkBasePath, bucketDateFormat);
        jsonHdfsSink
                .setBucketer(new MetaPathBucketer(bucketDateFormat))
                .setWriter(new JsonWriter())
                .setBatchSize(batchSize)
                .setBatchRolloverInterval(batchRolloverInterval)
                .setInactiveBucketCheckInterval(bucketCheckInterval)
                .setInactiveBucketThreshold(inactiveBucketThreshold)
                .setFSConfig(HdfsUtils.getConf())
                .setRollByDate(true)
                .setFinalSuffix(".json");
        sinkPathDataStringKeyedStream.addSink(jsonHdfsSink).name("kafka-hdfs-json");
        // applicaton execute
        env.execute(jobName);
    }
}
