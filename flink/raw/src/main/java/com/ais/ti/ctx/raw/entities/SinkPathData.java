package com.ais.ti.ctx.raw.entities;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * 解析topicValue
 */
public class SinkPathData implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nonnull
    private String path;
    @Nonnull
    private String topic;
    @Nonnull
    private String data;

    public SinkPathData(@Nonnull String path, @Nonnull String topic, @Nonnull String data) {
        this.path = path;
        this.topic = topic;
        this.data = data;
    }

    @Nonnull
    public String getPath() {
        return path;
    }

    @Nonnull
    public String getTopic() {
        return topic;
    }

    @Nonnull
    public String getData() {
        return data;
    }

    public void setPath(@Nonnull String path) {
        this.path = path;
    }

    public void setTopic(@Nonnull String topic) {
        this.topic = topic;
    }

    public void setData(@Nonnull String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "SinkPathData{"
                + "path='"
                + path
                + '\''
                + ", topic='"
                + topic
                + '\''
                + ", data='"
                + data
                + '\''
                + '}';
    }
}
