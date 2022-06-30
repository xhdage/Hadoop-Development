package com.ais.ti.ctx.raw.func.bucketer;

import com.ais.ti.ctx.raw.entities.SinkPathData;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class MetaPathBucketer implements Bucketer<SinkPathData> {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH-mm";

    private final String formatString;

    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.of("Asia/Shanghai");
    private ZoneId zoneId;

    private transient DateTimeFormatter dateTimeFormatter;

    /**
     * Creates a new {@code DateTimeBucketer} with format string {@code "yyyy-MM-dd--HH"} using JVM's
     * default timezone.
     */
    public MetaPathBucketer() {
        this(DEFAULT_FORMAT_STRING);
    }

    /**
     * Creates a new {@code DateTimeBucketer} with the given date/time format string using JVM's
     * default timezone.
     *
     * @param formatString The format string that will be given to {@code DateTimeFormatter} to
     *                     determine the bucket path.
     */
    public MetaPathBucketer(String formatString) {
        this(formatString, DEFAULT_ZONE_ID);
    }

    /**
     * Creates a new {@code DateTimeBucketer} with format string {@code "yyyy-MM-dd--HH"} using the
     * given timezone.
     *
     * @param zoneId The timezone used to format {@code DateTimeFormatter} for bucket path.
     */
    public MetaPathBucketer(ZoneId zoneId) {
        this(DEFAULT_FORMAT_STRING, zoneId);
    }

    /**
     * Creates a new {@code DateTimeBucketer} with the given date/time format string using the given
     * timezone.
     *
     * @param formatString The format string that will be given to {@code DateTimeFormatter} to
     *                     determine the bucket path.
     * @param zoneId       The timezone used to format {@code DateTimeFormatter} for bucket path.
     */
    public MetaPathBucketer(String formatString, ZoneId zoneId) {
        this.formatString = Preconditions.checkNotNull(formatString);
        this.zoneId = Preconditions.checkNotNull(zoneId);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.formatString).withZone(zoneId);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
    }

    @Override
    public Path getBucketPath(Clock clock, Path basePath, SinkPathData element) {
        String newDateTimeString =
                dateTimeFormatter.format(Instant.ofEpochMilli(clock.currentTimeMillis()));
        return new Path(String.format("%s/%s/%s", basePath, element.getPath(), newDateTimeString));
    }

    @Override
    public String toString() {
        return "MetaPathBucketer{"
                + "formatString='"
                + formatString
                + '\''
                + ", zoneId="
                + zoneId
                + ", dateTimeFormatter="
                + dateTimeFormatter
                + '}';
    }
}
