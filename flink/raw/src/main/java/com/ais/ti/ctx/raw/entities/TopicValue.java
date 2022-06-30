package com.ais.ti.ctx.raw.entities;

import javax.annotation.Nonnull;

/**
 * entity for kafka topic && value.
 */
public class TopicValue {
    @Nonnull
    private String topic;
    @Nonnull
    private String value;

    @Nonnull
    public String getTopic() {
        return topic;
    }

    @Nonnull
    public String getValue() {
        return value;
    }

    public void setTopic(@Nonnull String topic) {
        this.topic = topic;
    }

    public void setValue(@Nonnull String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "TopicValue{" + "topic='" + topic + '\'' + ", value='" + value + '\'' + '}';
    }
}
