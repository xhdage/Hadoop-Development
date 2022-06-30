package com.ais.ti.ctx.raw.entities;

import java.io.Serializable;

public class DataPairs implements Serializable {

    public static final long serialVersionUID = 1L;

    private Object data;
    private Object extraData;

    public Object getData() {
        return data;
    }

    public Object getExtraData() {
        return extraData;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public void setExtraData(Object extraData) {
        this.extraData = extraData;
    }
}
