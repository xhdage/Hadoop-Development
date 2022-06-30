package com.ais.ti.ctx.raw.entities;

public class KafkaFieldStateData {
    private boolean status;
    private String data;
    private String meta;
    private String extraData;

    public KafkaFieldStateData(boolean status) {
        this.status = status;
    }

    public KafkaFieldStateData(String data, String meta, String extraData) {
        this.data = data;
        this.meta = meta;
        this.extraData = extraData;
    }

    public KafkaFieldStateData(boolean status, String data, String meta, String extraData) {
        this.status = status;
        this.data = data;
        this.meta = meta;
        this.extraData = extraData;
    }

    public boolean getStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
    }

    public String getExtraData() {
        return extraData;
    }

    public void setExtraData(String extraData) {
        this.extraData = extraData;
    }

    @Override
    public String toString() {
        return "KafkaFieldStateData{"
                + "status="
                + status
                + ", data='"
                + data
                + '\''
                + ", meta='"
                + meta
                + '\''
                + ", extraData='"
                + extraData
                + '\''
                + '}';
    }
}
