package com.ais.ti.ctx.raw.utils;

public class PathUtils {

    /**
     * 异常数据的字段子路径保存。
     *
     * @param topic kafka topic。
     * @return sub-path.
     */
    public static String getErrSubPath(String topic) {
        return String.format(".error/%s", topic);
    }

    /**
     * @return sub-path of meta.
     */
    public static String getMetaSubPath() {
        return ".meta";
    }
}
