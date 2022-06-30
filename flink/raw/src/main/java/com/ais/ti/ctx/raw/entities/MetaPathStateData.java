package com.ais.ti.ctx.raw.entities;

public class MetaPathStateData {
    private boolean status;
    private String subPath;

    public MetaPathStateData(boolean status) {
        this.status = status;
    }

    public MetaPathStateData(boolean status, String subPath) {
        this.status = status;
        this.subPath = subPath;
    }

    public boolean getStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public String getSubPath() {
        return subPath;
    }

    public void setSubPath(String subPath) {
        this.subPath = subPath;
    }

    @Override
    public String toString() {
        return "MetaPathStateData{" + "status=" + status + ", subPath='" + subPath + '\'' + '}';
    }
}
