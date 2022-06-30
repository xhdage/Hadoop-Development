package com.ais.ti.ctx.raw.func.params;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Params {

    private ParameterTool parameterTool;

    // log
    private static final Logger logger = LoggerFactory.getLogger(Params.class);

    public Params(String[] args) {
        parameterTool = ParameterTool.fromArgs(args);
    }

    /**
     * 获取flink source并行度。
     */
    public int getEnvParallelism() {
        int envParallelism = parameterTool.getInt("env-parallelism", 2);
        if (envParallelism > 200 || envParallelism < 1) {
            logger.error("flink env parallelism is greater than {} or less than {}", 200, 1);
            throw new RuntimeException(
                    String.format("flink env parallelism is greater than %s or less than %s", 200, 1));
        }
        logger.info("env-parallelism = {}", envParallelism);
        return envParallelism;
    }

    /**
     * 获取flink source并行度。
     */
    public int getSourceParallelism() {
        int sourceParallelism = parameterTool.getInt("source-parallelism", 1);
        if (sourceParallelism > 200 || sourceParallelism < 1) {
            logger.error("flink source parallelism is greater than {} or less than {}", 200, 1);
            throw new RuntimeException(
                    String.format("flink source parallelism is greater than %s or less than %s", 200, 1));
        }
        logger.info("source-parallelism = {}", sourceParallelism);
        return sourceParallelism;
    }

    /**
     * 获取flink flatMap并行度。
     */
    public int getFlatMapParallelism() {
        int flatMapParallelism = parameterTool.getInt("flatMap-parallelism", 2);
        if (flatMapParallelism > 200 || flatMapParallelism < 1) {
            logger.error("flink flatMap parallelism is greater than {} or less than {}", 200, 1);
            throw new RuntimeException(
                    String.format("flink flatMap parallelism is greater than %s or less than %s", 200, 1));
        }
        logger.info("flatMap-parallelism = {}", flatMapParallelism);
        return flatMapParallelism;
    }

    /**
     * 获取flink flatMap并行度。
     */
    public int getSinkParallelism() {
        int sinkParallelism = parameterTool.getInt("sink-parallelism", 2);
        if (sinkParallelism > 200 || sinkParallelism < 1) {
            logger.error("flink sink parallelism is greater than {} or less than {}", 200, 1);
            throw new RuntimeException(
                    String.format("flink sink parallelism is greater than %s or less than %s", 200, 1));
        }
        logger.info("sink-parallelism = {}", sinkParallelism);
        return sinkParallelism;
    }

    /**
     * checkpoint间隔时间，单位ms。
     */
    public long getCheckpointInterval() {
        int checkpointInterval = parameterTool.getInt("checkpoint-interval", 5000);
        if (checkpointInterval > 60 * 1000L || checkpointInterval < 500) {
            logger.error(
                    "checkpoint-interval is longer than {} ms or shorter than {} ms", 60 * 1000L, 500);
            throw new RuntimeException(
                    String.format(
                            "checkpoint-interval is longer than %s ms or shorter than %s ms", 60 * 1000L, 500));
        }
        logger.info("checkpoint-interval = {}", checkpointInterval);
        return checkpointInterval;
    }

    /**
     * flink重试次数。
     */
    public int getRestartAttempts() {
        int restartAttempts = parameterTool.getInt("restart-attempt", 1);
        if (restartAttempts > 10 || restartAttempts < 0) {
            logger.error("flink application restart-attempt is greater than {} or less than {}", 10, 0);
            throw new RuntimeException(
                    String.format(
                            "flink application restart-attempt is greater than %s or less " + "than %s", 10, 0));
        }
        logger.info("restart-attempt = {}", restartAttempts);
        return restartAttempts;
    }

    /**
     * flink重试间隔时间，单位sec。
     */
    public int getRestartInterval() {
        int restartInterval = parameterTool.getInt("restart-interval", 5);
        if (restartInterval > 300 || restartInterval < 1) {
            logger.error("flink application restart-interval is greater than {} or less than {}", 300, 1);
            throw new RuntimeException(
                    String.format(
                            "flink application restart-interval is greater than %s or less " + "than %s",
                            300, 1));
        }
        logger.info("restart-interval = {}", restartInterval);
        return restartInterval;
    }

    /**
     * kafka bootstrap-server.
     */
    public String getKafkaServer() {
        String servers = parameterTool.get("bootstrap-server");
        if (servers == null) {
            logger.error("kafka bootstrap-server is required, but null");
            throw new RuntimeException("kafka bootstrap-server is required, but null");
        }
        logger.info("bootstrap-server = {}", servers);
        return servers;
    }

    /**
     * kafka groupId.
     */
    public String getKafkaGroupId() {
        String groupId = parameterTool.get("groupId");
        if (groupId == null) {
            logger.error("kafka groupId is required, but null");
            throw new RuntimeException("kafka groupId is required, but null");
        }
        logger.info("groupId = {}", groupId);
        return groupId;
    }

    /**
     * kafka topic的正则表达式。
     */
    public String getKafkaTopicPattern() {
        String topicPattern = parameterTool.get("topic-pattern");
        if (topicPattern == null) {
            logger.error("kafka topic-pattern is required, but null");
            throw new RuntimeException("kafka topic-pattern is required, but null");
        }
        logger.info("topic-pattern = {}", topicPattern);
        return topicPattern;
    }

    /**
     * data sink base-path。
     */
    public String getSinkBasePath() {
        String basePath = parameterTool.get("base-path");
        if (basePath == null) {
            logger.error("sink base-path is required, but null");
            throw new RuntimeException("sink base-path  is required, but null");
        }
        logger.info("base-path = {}", basePath);
        return basePath;
    }

    /**
     * flink job name.
     */
    public String getJobName() {
        String jobName = parameterTool.get("job-name", "kafkaRawJsonSink");
        logger.info("job-name = {}", jobName);
        return jobName;
    }

    /**
     * kafka partition discovery interval（ms）。
     */
    public String getPartitionDiscoveryInterval() {
        String interval = parameterTool.get("partition-discovery-interval", "60000");
        Long value = Long.valueOf(interval);
        if (value > 10 * 60 * 1000L || value < 10 * 1000L) {
            logger.error(
                    "partition-discovery-interval is greater than {} or less than {}",
                    10 * 60 * 1000L,
                    10 * 1000L);
            throw new RuntimeException(
                    String.format(
                            "partition-discovery-interval is greater than %s or less " + "than %s",
                            10 * 60 * 1000L, 10 * 1000L));
        }
        logger.info("partition-discovery-interval = {}", value);
        return interval;
    }

    /**
     * 设置桶的日期格式。
     */
    public String getBucketDateFormat() {
        String dateFormat = parameterTool.get("bucket-dateformat", "yyyy-MM-dd");
        logger.info("bucket-dateformat = {}", dateFormat);
        return dateFormat;
    }

    /**
     * 滚动文件大小。
     */
    public long getBatchSize() {
        long batchSize = parameterTool.getLong("batch-size", 1024 * 1024L * 256);
        if (batchSize > 8 * 1024 * 1024L * 1024 || batchSize < 1024) {
            logger.error("batch size is greater than {} or less than {}", 8 * 1024 * 1024L * 1024, 1024);
            throw new RuntimeException(
                    String.format(
                            "batch size is greater than %s or less than %s", 8 * 1024 * 1024L * 1024, 1024));
        }
        logger.info("batch-size = {}", batchSize);
        return batchSize;
    }

    /**
     * 滚动的时间间隔。(ms)
     */
    public long getBatchRolloverInterval() {
        long rolloverInterval = parameterTool.getLong("rollover-interval", 24 * 60 * 60 * 1000L);
        if (rolloverInterval > 7 * 24 * 60 * 60 * 1000L || rolloverInterval < 60 * 1000L) {
            logger.error(
                    "batch rollover interval is greater than {} or less than {}",
                    7 * 24 * 60 * 60 * 1000L,
                    60 * 1000L);
            throw new RuntimeException(
                    String.format(
                            "batch rollover interval is greater than %s or less than %s",
                            7 * 24 * 60 * 60 * 1000L, 60 * 1000L));
        }
        logger.info("rollover-interval = {}", rolloverInterval);
        return rolloverInterval;
    }

    /**
     * 定时检查bucket.
     */
    public long getBucketCheckInterval() {
        long checkInterval = parameterTool.getLong("check-interval", 2 * 60 * 1000L);
        if (checkInterval > 5 * 60 * 1000L || checkInterval < 30 * 1000L) {
            logger.error(
                    "batch check interval is greater than {} or less than {}", 5 * 60 * 1000L, 30 * 1000L);
            throw new RuntimeException(
                    String.format(
                            "batch check interval is greater than %s or less than %s",
                            5 * 60 * 1000L, 30 * 1000L));
        }
        logger.info("check-interval = {}", checkInterval);
        return checkInterval;
    }

    /**
     * 文件多久不活跃则关闭。（ms）
     */
    public long getInactiveBucketThreshold() {
        long inactiveBucketThreshold =
                parameterTool.getLong("inactive-bucketThreshold", 5 * 60 * 1000L);
        if (inactiveBucketThreshold > 10 * 60 * 1000L || inactiveBucketThreshold < 30 * 1000L) {
            logger.error(
                    "batch inactive-bucketThreshold is greater than {} or less than {}",
                    10 * 60 * 1000L,
                    30 * 1000L);
            throw new RuntimeException(
                    String.format(
                            "batch inactive-bucketThreshold is greater than %s or less than " + "%s",
                            10 * 60 * 1000L, 30 * 1000L));
        }
        logger.info("inactive-bucketThreshold = {}", inactiveBucketThreshold);
        return inactiveBucketThreshold;
    }

    public String getCheckpointDir() {
        String checkpointsDir = parameterTool.get("checkpoints-dir");
        if (checkpointsDir == null) {
            logger.error("flink state checkpoints-dir is required, but null");
            throw new RuntimeException("flink state checkpoints-dir is required, but null");
        }
        logger.info("checkpoints-dir = {}", checkpointsDir);
        return checkpointsDir;
    }

    public static void main(String[] args) {
        Params params = new Params(args);
        //        System.out.println(params.getParallelism());
    }
}
