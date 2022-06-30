package com.ais.ti.ctx.raw.entities;

import java.util.Objects;

public class MetaPairs {
    private int priority;
    private String value;

    public MetaPairs(int priority, String value) {
        this.priority = priority;
        this.value = value;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MetaPairs{" + "priority=" + priority + ", value='" + value + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaPairs metaPairs = (MetaPairs) o;
        return priority == metaPairs.priority && value.equals(metaPairs.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(priority, value);
    }
}
