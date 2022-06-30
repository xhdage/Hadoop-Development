package com.ais.ti.ctx.raw.sink;

import com.ais.ti.ctx.raw.func.writer.IWriter;
import com.ais.ti.ctx.raw.func.writer.StringFsWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@SuppressWarnings("DuplicatedCode")
public class SimpleBucketingSink<T> extends RichSinkFunction<T> implements ProcessingTimeCallback {

    private static final Logger logger = LoggerFactory.getLogger(SimpleBucketingSink.class);

    // --------------------------------------------------------------------------------------------
    //  User configuration values
    // --------------------------------------------------------------------------------------------
    // These are initialized with some defaults but are meant to be changeable by the user

    /**
     * The state object that is handled by Flink from snapshot/restore. This contains state for every
     * open bucket: the current in-progress part file path, its valid length and the pending part
     * files.
     */
    private transient SimpleBucketingSink.State<T> state;

    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.of("Asia/Shanghai");
    private ZoneId zoneId = DEFAULT_ZONE_ID;

    /**
     * format:yyyyMMddHHmmss
     */
    private transient DateTimeFormatter timeFormat;

    /**
     * format:yyyyMMdd
     */
    private transient DateTimeFormatter dateFormat;

    /**
     * The FileSystem reference.
     */
    private transient FileSystem fs;

    private transient Clock clock;

    private transient ProcessingTimeService processingTimeService;

    /**
     * The default time between checks for inactive buckets. By default, {60 sec}.
     */
    private static final long DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS = 60 * 1000L;

    private long inactiveBucketCheckInterval = DEFAULT_INACTIVE_BUCKET_CHECK_INTERVAL_MS;

    /**
     * The default maximum size of part files (currently {@code 384 MB}).
     */
    private static final long DEFAULT_BATCH_SIZE = 1024L * 1024L * 256L;

    private long batchSize = DEFAULT_BATCH_SIZE;

    /**
     * The default threshold (in {@code ms}) for marking a bucket as inactive and closing its part
     * files. By default, {60 sec}.
     */
    private static final long DEFAULT_INACTIVE_BUCKET_THRESHOLD_MS = 60 * 1000L;

    private long inactiveBucketThreshold = DEFAULT_INACTIVE_BUCKET_THRESHOLD_MS;

    /**
     * The default time interval at which part files are written to the filesystem.
     */
    private static final long DEFAULT_BATCH_ROLLOVER_INTERVAL = 24 * 60 * 60 * 1000L;

    private long batchRolloverInterval = DEFAULT_BATCH_ROLLOVER_INTERVAL;

    // 是否以日期实现滚动
    private static final boolean DEFAULT_ROLL_BY_DATE = true;
    private boolean isRollByDate = DEFAULT_ROLL_BY_DATE;

    /**
     * The suffix for {@code in-progress} part files. These are files we are currently writing to, but
     * which were not yet confirmed by a checkpoint.
     */
    private static final String DEFAULT_IN_PROGRESS_SUFFIX = ".in-progress";

    /**
     * The prefix for {@code in-progress} part files. These are files we are currently writing to, but
     * which were not yet confirmed by a checkpoint.
     */
    private static final String DEFAULT_IN_PROGRESS_PREFIX = "_";

    // These are the actually configured prefixes/suffixes
    private String inProgressSuffix = DEFAULT_IN_PROGRESS_SUFFIX;
    private String inProgressPrefix = DEFAULT_IN_PROGRESS_PREFIX;

    /**
     * The suffix for {@code pending} part files. These are closed files that we are not currently
     * writing to (inactive or reached {@link #batchSize}), but which were not yet confirmed by a
     * checkpoint.
     */
    private static final String DEFAULT_PENDING_SUFFIX = ".pending";

    /**
     * The prefix for {@code pending} part files. These are closed files that we are not currently
     * writing to (inactive or reached {@link #batchSize}), but which were not yet confirmed by a
     * checkpoint.
     */
    private static final String DEFAULT_PENDING_PREFIX = "_";

    private String pendingSuffix = DEFAULT_PENDING_SUFFIX;
    private String pendingPrefix = DEFAULT_PENDING_PREFIX;

    private static final String DEFAULT_FINAL_SUFFIX = "";
    private static final String DEFAULT_FINAL_PREFIX = "";

    private String finalSuffix = DEFAULT_FINAL_SUFFIX;
    private String finalPrefix = DEFAULT_FINAL_PREFIX;

    /**
     * The default prefix/suffix for part files.
     */
    private static final String DEFAULT_PART_SUFFIX = null;

    private String partSuffix = DEFAULT_PART_SUFFIX;

    private static final String DEFAULT_PART_PREFIX = "part";
    private String partPrefix = DEFAULT_PART_PREFIX;

    /**
     * The base {@code Path} that stores all bucket directories.
     */
    private final String basePath;

    /**
     * The {@code Bucketer} that is used to determine the path of bucket directories.
     */
    private Bucketer<T> bucketer;

    /**
     * We have a template and call duplicate() for each parallel writer in open() to get the actual
     * writer that is used for the part files.
     */
    private IWriter<T> writerTemplate;

    /**
     * User-defined FileSystem parameters.
     */
    @Nullable
    private Configuration fsConfig;

    private static final String DEFAULT_TIME_FORMAT = "yyyyMMddHHmmss";
    private String timeFormatString = DEFAULT_TIME_FORMAT;

    private static final String DEFAULT_DATE_FORMAT = "yyyyMMdd";
    private String dateFormatString = DEFAULT_DATE_FORMAT;

    /**
     * Creates a new {@code CustomBucketingSink} that writes files to the given base directory.
     *
     * <p>This uses a{@link DateTimeBucketer} as {@link Bucketer} and a {@link StringFsWriter} has
     * writer. The maximum bucket size is set to 384 MB.
     *
     * @param basePath The directory to which to write the bucket files.
     */
    public SimpleBucketingSink(String basePath) throws IOException {
        this.basePath = basePath;
        this.bucketer = new DateTimeBucketer<>();
        this.writerTemplate = new StringFsWriter<>();
    }

    public SimpleBucketingSink(String basePath, String dateFormat) throws IOException {
        this(basePath);
        this.dateFormatString = dateFormat;
    }

    /**
     * Specify a custom {@code Configuration} that will be used when creating the {@link FileSystem}
     * for writing.
     */
    public SimpleBucketingSink<T> setFSConfig(org.apache.hadoop.conf.Configuration config) {
        this.fsConfig = new Configuration();
        for (Map.Entry<String, String> entry : config) {
            fsConfig.setString(entry.getKey(), entry.getValue());
        }
        return this;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initFileSystem(this.fsConfig);
        timeFormat = DateTimeFormatter.ofPattern(timeFormatString).withZone(zoneId);
        dateFormat = DateTimeFormatter.ofPattern(dateFormatString).withZone(zoneId);
        state = new SimpleBucketingSink.State<>();
        processingTimeService =
                ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();

        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

        processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);

        this.clock =
                new Clock() {
                    @Override
                    public long currentTimeMillis() {
                        return processingTimeService.getCurrentProcessingTime();
                    }
                };
        logger.info("sink open.");
    }

    @Override
    public void close() throws Exception {
        if (state != null) {
            for (Map.Entry<String, SimpleBucketingSink.BucketState<T>> entry :
                    state.bucketStates.entrySet()) {
                closeAllPartFile(entry.getValue());
            }
        }
        logger.info("sink close.");
    }

    /**
     * Returns {@code true} if the current {@code part-file} should be closed and a new should be
     * created. This happens if:
     *
     * <ol>
     *   <li>no file is created yet for the task to write to, or
     *   <li>the current file has reached the maximum bucket size.
     *   <li>the current file is older than roll over interval
     * </ol>
     */
    private boolean shouldRoll(SimpleBucketingSink.BucketState<T> bucketState) throws IOException {
        boolean shouldRoll = false;
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        long writePosition = bucketState.writer.getPos();
        if (writePosition > batchSize) {
            shouldRoll = true;
            logger.debug(
                    "SimpleBucketingSink {} starting new bucket because file position {} is above batch size {}.",
                    subtaskIndex,
                    writePosition,
                    batchSize);
        }
        return shouldRoll;
    }

    private Path getInProgressPathFor(Path path) {
        return new Path(path.getParent(), inProgressPrefix + path.getName()).suffix(inProgressSuffix);
    }

    private Path getPendingPathFor(Path path) {
        return new Path(path.getParent(), pendingPrefix + path.getName()).suffix(pendingSuffix);
    }

    private Path getFinalPathFor(Path path) {
        return new Path(path.getParent(), finalPrefix + path.getName()).suffix(finalSuffix);
    }

    private Path assemblePartPath(Path bucket, int subtaskIndex) {
        String localPartSuffix = partSuffix != null ? "-" + partSuffix : "";
        return new Path(
                bucket,
                String.format(
                        "%s-%s_%s%s",
                        partPrefix,
                        timeFormat.format(Instant.ofEpochMilli(clock.currentTimeMillis())),
                        subtaskIndex,
                        localPartSuffix));
    }

    /**
     * 关闭 Closes the current part file and moves it from the in-progress state to the pending state.
     */
    private void closeCurrentPartFile(SimpleBucketingSink.BucketState<T> bucketState)
            throws Exception {
        // 如果是打开的，则关闭
        if (bucketState.isWriterOpen) {
            bucketState.writer.close();
            bucketState.isWriterOpen = false;
            if (bucketState.currentFile != null) {
                logger.debug("Close in-progress bucket {} writer.", bucketState.currentFile);
            }
        }
        // 如果当前文件不为null。
        if (bucketState.currentFile != null) {
            Path currentPartPath = new Path(bucketState.currentFile);
            Path inProgressPath = getInProgressPathFor(currentPartPath);
            Path pendingPath = getPendingPathFor(currentPartPath);

            fs.rename(inProgressPath, pendingPath);
            logger.debug("Moving in-progress bucket {} to pending file {}", inProgressPath, pendingPath);
            bucketState.pendingFiles.add(currentPartPath.toString());
            bucketState.currentFile = null;
        }
    }

    /**
     * 关闭所有的文件，并重命名为最终的文件。
     */
    private void closeAllPartFile(SimpleBucketingSink.BucketState<T> bucketState) throws IOException {
        // 如果是打开的，则关闭
        if (bucketState.isWriterOpen) {
            bucketState.writer.close();
            bucketState.isWriterOpen = false;
            if (bucketState.currentFile != null) {
                logger.debug("Close in-progress bucket {} writer.", bucketState.currentFile);
            }
        }
        // 如果当前文件不为null。
        if (bucketState.currentFile != null) {
            Path currentPartPath = new Path(bucketState.currentFile);
            Path inProgressPath = getInProgressPathFor(currentPartPath);
            Path finalPath = getFinalPathFor(currentPartPath);

            fs.rename(inProgressPath, finalPath);
            logger.debug("Moving in-progress bucket {} to final file {}", inProgressPath, finalPath);
            bucketState.currentFile = null;
        }

        List<String> pendingFiles = bucketState.pendingFiles;
        for (String pendingFile : pendingFiles) {
            Path path = new Path(pendingFile);
            Path pendingPath = getPendingPathFor(path);
            Path finalPath = getFinalPathFor(path);
            fs.rename(pendingPath, finalPath);
            logger.debug("Moving pending bucket {} to final file {}", pendingPath, finalPath);
        }
        bucketState.pendingFiles.clear();
    }

    /**
     * 在设定时间内没数据写入的文件则关闭。
     */
    private void closeExpiredFile(SimpleBucketingSink.BucketState<T> bucketState) throws IOException {
        // 如果是打开的，则关闭
        if (bucketState.isWriterOpen && !bucketState.isWriting) {
            bucketState.writer.close();
            bucketState.isWriterOpen = false;
            bucketState.isExpiredClosed = true;
        }
    }

    /**
     * 检查并关闭前一天的文件。
     */
    private void closePartFileByRollDate(SimpleBucketingSink.BucketState<T> bucketState)
            throws IOException {
        // 如果是打开的，则关闭
        if (bucketState.isWriting) {
            logger.debug("The file '{}' is writing, can not close!", bucketState.currentFile);
            return;
        }
        if (bucketState.isWriterOpen) {
            bucketState.writer.close();
            bucketState.isWriterOpen = false;
        }
        // 如果当前文件不为null。
        if (bucketState.currentFile != null) {
            Path currentPartPath = new Path(bucketState.currentFile);
            Path inProgressPath = getInProgressPathFor(currentPartPath);
            Path pendingPath = getPendingPathFor(currentPartPath);

            fs.rename(inProgressPath, pendingPath);
            logger.debug("Moving in-progress bucket {} to pending file {}", inProgressPath, pendingPath);
            bucketState.pendingFiles.add(currentPartPath.toString());
            bucketState.currentFile = null;
        }
    }

    /**
     * 将append文件改为final文件，真正完成一次文件的写入。
     */
    private void renameAppendFile(SimpleBucketingSink.BucketState<T> bucketState) throws IOException {
        List<String> pendingFiles = bucketState.pendingFiles;
        for (String pendingFie : pendingFiles) {
            Path path = new Path(pendingFie);
            Path pendingPathFor = getPendingPathFor(path);
            Path finalPathFor = getFinalPathFor(path);
            fs.rename(pendingPathFor, finalPathFor);
            logger.debug("Moving pending bucket {} to final file {}", pendingPathFor, finalPathFor);
        }
        bucketState.pendingFiles.clear();
    }

    /**
     * re-open inProgressPath file for append to file.
     */
    private void reOpenPartFile(Path bucketPath, SimpleBucketingSink.BucketState<T> bucketState)
            throws IOException {
        if (bucketState.currentFile == null) {
            logger.error("The current file of bucket-path '{}' is null", bucketPath.toString());
            throw new RuntimeException(
                    String.format("The current file of bucket-path '%s' is null", bucketPath.toString()));
        }
        if (bucketState.isWriterOpen) {
            logger.error("The part file '{}' is already open", bucketState.currentFile);
            throw new RuntimeException(
                    String.format("The part file '%s' is already open", bucketState.currentFile));
        }

        Path partPath = new Path(bucketState.currentFile);
        Path inProgressPath = getInProgressPathFor(partPath);
        if (!fs.exists(inProgressPath)) {
            logger.error("The in-process file '{}' is not exists", inProgressPath.toString());
            throw new RuntimeException(
                    String.format("The in-process file '%s' is not exists", inProgressPath.toString()));
        }
        bucketState.writer.reOpen(fs, inProgressPath);
        bucketState.isWriterOpen = true;
        bucketState.isExpiredClosed = false;
    }

    /**
     * Closes the current part file and opens a new one with a new bucket path, as returned by the
     * {@link Bucketer}. If the bucket is not new, then this will create a new file with the same path
     * as its predecessor, but with an increased rolling counter (see {@link SimpleBucketingSink}.
     */
    private void openNewPartFile(Path bucketPath, SimpleBucketingSink.BucketState<T> bucketState)
            throws Exception {
        if (!fs.exists(bucketPath)) {
            try {
                if (fs.mkdirs(bucketPath)) {
                    logger.debug("Created new bucket directory: {}", bucketPath);
                }
            } catch (IOException e) {
                throw new RuntimeException("Could not create new bucket path.", e);
            }
        }

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        Path partPath = assemblePartPath(bucketPath, subtaskIndex);
        if (fs.exists(partPath)
                || fs.exists(getPendingPathFor(partPath))
                || fs.exists(getInProgressPathFor(partPath))
                || fs.exists(getFinalPathFor(partPath))) {
            logger.error("Exists part file name: {}", partPath.toString());
            throw new Exception(String.format("Exists part file name: %s", partPath.toString()));
        }

        // Record the creation time of the bucket
        bucketState.creationTime = processingTimeService.getCurrentProcessingTime();
        bucketState.creationDateString =
                dateFormat.format(Instant.ofEpochMilli(clock.currentTimeMillis()));

        logger.debug("Next part path is {}", partPath.toString());
        bucketState.currentFile = partPath.toString();

        Path inProgressPath = getInProgressPathFor(partPath);
        if (bucketState.writer == null) {
            bucketState.writer = writerTemplate.duplicate();
            if (bucketState.writer == null) {
                throw new UnsupportedOperationException(
                        "Could not duplicate writer. "
                                + "Class '"
                                + writerTemplate.getClass().getCanonicalName()
                                + "' must implement the "
                                + "'Writer.duplicate()' method.");
            }
        }

        bucketState.writer.open(fs, inProgressPath);
        bucketState.isWriterOpen = true;
        bucketState.isExpiredClosed = false;
    }

    /**
     * 根据定时器间隔时间，检查文件；如果inProcess文件在设定范围内没有数据写入，则关闭；将pending文件rename成final文件。
     */
    private void closePartFilesByTime(long currentProcessingTime) throws Exception {
        String format = dateFormat.format(Instant.ofEpochMilli(clock.currentTimeMillis()));
        synchronized (state.bucketStates) {
            LinkedList<Path> invalidBucketPaths = new LinkedList<>();
            for (Map.Entry<String, SimpleBucketingSink.BucketState<T>> entry :
                    state.bucketStates.entrySet()) {
                BucketState<T> state = entry.getValue();
                Path buckPath = new Path(entry.getKey());
                if ((state.lastWrittenToTime < currentProcessingTime - inactiveBucketThreshold)) {
                    // 如果在设定时间没数据写入，关闭文件。
                    logger.debug(
                            "SimpleBucketingSink {} closing bucket due to inactivity of over {} ms.",
                            getRuntimeContext().getIndexOfThisSubtask(),
                            inactiveBucketThreshold);
                    closeExpiredFile(state);
                }
                // 检查该文件是否为昨天的文件, rename pending文件。
                if (isRollByDate && !format.equals(state.creationDateString)) {
                    closePartFileByRollDate(state);
                }
                // 如果超过batchRolloverInterval时间
                if (state.creationTime < currentProcessingTime - batchRolloverInterval) {
                    closePartFileByRollDate(state);
                }
                // 将appending文件改名
                renameAppendFile(state);
                // 检查哪些bucketState无效
                if (!state.isWriting
                        && state.currentFile == null
                        && !state.isWriterOpen
                        && state.pendingFiles.isEmpty()) {
                    invalidBucketPaths.add(buckPath);
                }
            }
            // 从map中删除为空的state。
            for (Path bucketPath : invalidBucketPaths) {
                state.removeBucketState(bucketPath);
                logger.debug(
                        "SimpleBucketingSink buckPath {} remove bucket due to inactivity.",
                        bucketPath.toString());
            }
            logger.debug(state.bucketStates.toString());
        }
    }

    @Override
    public void invoke(T value) throws Exception {
        Path bucketPath = bucketer.getBucketPath(clock, new Path(basePath), value);

        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

        SimpleBucketingSink.BucketState<T> bucketState = state.getBucketState(bucketPath);
        if (bucketState == null) {
            bucketState = new SimpleBucketingSink.BucketState<>(currentProcessingTime);
            bucketState.isWriting = true;
            state.addBucketState(bucketPath, bucketState);
        }

        // 设置数据准备写入,防止再写入过程中
        bucketState.isWriting = true;
        // 文件是否初始化
        if (bucketState.currentFile == null) {
            openNewPartFile(bucketPath, bucketState);
        }
        // 文件因为超时被关闭，则打开。
        if (bucketState.isExpiredClosed) {
            reOpenPartFile(bucketPath, bucketState);
        }
        // 文件太大
        if (shouldRoll(bucketState)) {
            closeCurrentPartFile(bucketState);
            openNewPartFile(bucketPath, bucketState);
        }
        bucketState.writer.write(value);
        bucketState.lastWrittenToTime = currentProcessingTime;
        // 关闭正在写入。
        bucketState.isWriting = false;
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        closePartFilesByTime(currentProcessingTime);
        processingTimeService.registerTimer(currentProcessingTime + inactiveBucketCheckInterval, this);
        logger.debug("onProcessingTime");
    }

    /**
     * Create a file system with the user-defined {@code HDFS} configuration.
     */
    private void initFileSystem(Configuration extraUserConf) throws IOException {
        if (fs == null) {
            Path path = new Path(basePath);
            fs = createHadoopFileSystem(path, extraUserConf);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Setters for User configuration values
    // --------------------------------------------------------------------------------------------

    /**
     * Sets the maximum bucket size in bytes.
     *
     * <p>When a bucket part file becomes larger than this size a new bucket part file is started and
     * the old one is closed. The name of the bucket files depends on the {@link Bucketer}.
     *
     * @param batchSize The bucket part file size in bytes.
     */
    public SimpleBucketingSink<T> setBatchSize(long batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Sets the roll over interval in milliseconds.
     *
     * <p>When a bucket part file is older than the roll over interval, a new bucket part file is
     * started and the old one is closed. The name of the bucket file depends on the {@link Bucketer}.
     * Additionally, the old part file is also closed if the bucket is not written to for a minimum of
     * {@code inactiveBucketThreshold} ms.
     *
     * @param batchRolloverInterval The roll over interval in milliseconds
     */
    public SimpleBucketingSink<T> setBatchRolloverInterval(long batchRolloverInterval) {
        if (batchRolloverInterval > 0) {
            this.batchRolloverInterval = batchRolloverInterval;
        }
        return this;
    }

    /**
     * Sets the default time between checks for inactive buckets.
     *
     * @param interval The timeout, in milliseconds.
     */
    public SimpleBucketingSink<T> setInactiveBucketCheckInterval(long interval) {
        this.inactiveBucketCheckInterval = interval;
        return this;
    }

    /**
     * Sets the default threshold for marking a bucket as inactive and closing its part files. Buckets
     * which haven't been written to for at least this period of time become inactive. Additionally,
     * part files for the bucket are also closed if the bucket is older than {@code
     * batchRolloverInterval} ms.
     *
     * @param threshold The timeout, in milliseconds.
     */
    public SimpleBucketingSink<T> setInactiveBucketThreshold(long threshold) {
        this.inactiveBucketThreshold = threshold;
        return this;
    }

    /**
     * Sets the {@link Bucketer} to use for determining the bucket files to write to.
     *
     * @param bucketer The bucketer to use.
     */
    public SimpleBucketingSink<T> setBucketer(Bucketer<T> bucketer) {
        this.bucketer = bucketer;
        return this;
    }

    /**
     * Sets the {@link IWriter} to be used for writing the incoming elements to bucket files.
     *
     * @param writer The {@code Writer} to use.
     */
    public SimpleBucketingSink<T> setWriter(IWriter<T> writer) {
        this.writerTemplate = writer;
        return this;
    }

    /**
     * Sets the suffix of in-progress part files. The default is {@code ".in-progress"}.
     */
    public SimpleBucketingSink<T> setInProgressSuffix(String inProgressSuffix) {
        this.inProgressSuffix = inProgressSuffix;
        return this;
    }

    /**
     * Sets the prefix of in-progress part files. The default is {@code "_"}.
     */
    public SimpleBucketingSink<T> setInProgressPrefix(String inProgressPrefix) {
        this.inProgressPrefix = inProgressPrefix;
        return this;
    }

    /**
     * Sets the suffix of pending part files. The default is {@code ".pending"}.
     */
    public SimpleBucketingSink<T> setPendingSuffix(String pendingSuffix) {
        this.pendingSuffix = pendingSuffix;
        return this;
    }

    /**
     * Sets the prefix of pending part files. The default is {@code "_"}.
     */
    public SimpleBucketingSink<T> setPendingPrefix(String pendingPrefix) {
        this.pendingPrefix = pendingPrefix;
        return this;
    }

    /**
     * Sets the suffix of part files. The default is no suffix.
     */
    public SimpleBucketingSink<T> setPartSuffix(String partSuffix) {
        this.partSuffix = partSuffix;
        return this;
    }

    /**
     * Sets the prefix of part files. The default is {@code "part"}.
     */
    public SimpleBucketingSink<T> setPartPrefix(String partPrefix) {
        this.partPrefix = partPrefix;
        return this;
    }

    /**
     * 超过0.00时刻，对前一天的文件关闭，即按照日期过期。
     */
    public SimpleBucketingSink<T> setRollByDate(boolean byDate) {
        this.isRollByDate = byDate;
        return this;
    }

    public void setFinalSuffix(String finalSuffix) {
        this.finalSuffix = finalSuffix;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static FileSystem createHadoopFileSystem(Path path, @Nullable Configuration extraUserConf)
            throws IOException {

        // try to get the Hadoop File System via the Flink File Systems
        // that way we get the proper configuration

        final org.apache.flink.core.fs.FileSystem flinkFs =
                org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(path.toUri());
        final FileSystem hadoopFs =
                (flinkFs instanceof HadoopFileSystem)
                        ? ((HadoopFileSystem) flinkFs).getHadoopFileSystem()
                        : null;

        // fast path: if the Flink file system wraps Hadoop anyways and we need no extra config,
        // then we use it directly
        if (extraUserConf == null && hadoopFs != null) {
            return hadoopFs;
        } else {
            // we need to re-instantiate the Hadoop file system, because we either have
            // a special config, or the Path gave us a Flink FS that is not backed by
            // Hadoop (like file://)

            final org.apache.hadoop.conf.Configuration hadoopConf;
            if (hadoopFs != null) {
                // have a Hadoop FS but need to apply extra config
                hadoopConf = hadoopFs.getConf();
            } else {
                // the Path gave us a Flink FS that is not backed by Hadoop (like file://)
                // we need to get access to the Hadoop file system first

                // we access the Hadoop FS in Flink, which carries the proper
                // Hadoop configuration. we should get rid of this once the bucketing sink is
                // properly implemented against Flink's FS abstraction

                URI genericHdfsUri = URI.create("hdfs://localhost:12345/");
                org.apache.flink.core.fs.FileSystem accessor =
                        org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(genericHdfsUri);

                if (!(accessor instanceof HadoopFileSystem)) {
                    throw new IOException(
                            "Cannot instantiate a Hadoop file system to access the Hadoop configuration. "
                                    + "FS for hdfs:// is "
                                    + accessor.getClass().getName());
                }

                hadoopConf = ((HadoopFileSystem) accessor).getHadoopFileSystem().getConf();
            }

            // finalize the configuration

            final org.apache.hadoop.conf.Configuration finalConf;
            if (extraUserConf == null) {
                finalConf = hadoopConf;
            } else {
                finalConf = new org.apache.hadoop.conf.Configuration(hadoopConf);

                for (String key : extraUserConf.keySet()) {
                    finalConf.set(key, extraUserConf.getString(key, null));
                }
            }

            // we explicitly re-instantiate the file system here in order to make sure
            // that the configuration is applied.

            URI fsUri = path.toUri();
            final String scheme = fsUri.getScheme();
            final String authority = fsUri.getAuthority();

            if (scheme == null && authority == null) {
                fsUri = FileSystem.getDefaultUri(finalConf);
            } else if (scheme != null && authority == null) {
                URI defaultUri = FileSystem.getDefaultUri(finalConf);
                if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                    fsUri = defaultUri;
                }
            }

            final Class<? extends FileSystem> fsClass =
                    FileSystem.getFileSystemClass(fsUri.getScheme(), finalConf);
            final FileSystem fs;
            try {
                fs = fsClass.newInstance();
            } catch (Exception e) {
                throw new IOException("Cannot instantiate the Hadoop file system", e);
            }

            fs.initialize(fsUri, finalConf);

            // We don't perform checksums on Hadoop's local filesystem and use the raw filesystem.
            // Otherwise buffers are not flushed entirely during checkpointing which results in data loss.
            if (fs instanceof LocalFileSystem) {
                return ((LocalFileSystem) fs).getRaw();
            }
            return fs;
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Internal Classes
    // --------------------------------------------------------------------------------------------

    /**
     * This is used during snapshot/restore to keep track of in-progress buckets. For each bucket, we
     * maintain a state.
     */
    static final class State<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * For every bucket directory (key), we maintain a bucket state (value).
         */
        final Map<String, BucketState<T>> bucketStates = new HashMap<>();

        void addBucketState(Path bucketPath, SimpleBucketingSink.BucketState<T> state) {
            synchronized (bucketStates) {
                bucketStates.put(bucketPath.toString(), state);
            }
        }

        SimpleBucketingSink.BucketState<T> getBucketState(Path bucketPath) {
            synchronized (bucketStates) {
                return bucketStates.get(bucketPath.toString());
            }
        }

        void removeBucketState(Path bucketPath) {
            synchronized (bucketStates) {
                bucketStates.remove(bucketPath.toString());
            }
        }

        @Override
        public String toString() {
            return bucketStates.toString();
        }
    }

    /**
     * This is used for keeping track of the current in-progress buckets and files that we mark for
     * moving from pending to final location after we get a checkpoint-complete notification.
     */
    static final class BucketState<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * The file that was in-progress when the last checkpoint occurred.
         */
        String currentFile;

        /**
         * The time this bucket was last written to.
         */
        long lastWrittenToTime;

        /**
         * The time this bucket was created.
         */
        long creationTime;

        /**
         * The date this bucket was created. <>yyyyMMdd</>
         */
        String creationDateString;

        /**
         * Pending files that accumulated since the last checkpoint.
         */
        List<String> pendingFiles = new ArrayList<>();

        /**
         * Tracks if the writer is currently opened or closed.
         */
        private transient volatile boolean isWriterOpen = false;

        /**
         * 当前有数据准备写入。
         */
        private transient volatile boolean isWriting = false;

        /**
         * Tracks if the writer is currently opened or closed for expired.
         */
        private transient volatile boolean isExpiredClosed = false;

        /**
         * The actual writer that we user for writing the part files.
         */
        private transient IWriter<T> writer;

        BucketState(long lastWrittenToTime) {
            this.lastWrittenToTime = lastWrittenToTime;
        }

        @Override
        public String toString() {
            return "BucketState{"
                    + "currentFile='"
                    + currentFile
                    + '\''
                    + ", lastWrittenToTime="
                    + lastWrittenToTime
                    + ", creationTime="
                    + creationTime
                    + ", creationDateString='"
                    + creationDateString
                    + '\''
                    + ", pendingFiles="
                    + pendingFiles
                    + ", isWriterOpen="
                    + isWriterOpen
                    + ", isWriting="
                    + isWriting
                    + ", isExpiredClosed="
                    + isExpiredClosed
                    + '}';
        }
    }
}
