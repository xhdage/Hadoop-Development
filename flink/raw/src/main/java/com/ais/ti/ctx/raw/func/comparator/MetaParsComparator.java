package com.ais.ti.ctx.raw.func.comparator;

import com.ais.ti.ctx.raw.entities.MetaPairs;

import java.util.Comparator;

public class MetaParsComparator implements Comparator<MetaPairs> {
    // priority最大得为一级目录
    @Override
    public int compare(MetaPairs meta, MetaPairs other) {
        if (meta.getPriority() < other.getPriority()) {
            return 1;
        } else if (meta.getPriority() > other.getPriority()) {
            return -1;
        } else {
            if (meta.getValue().compareTo(other.getValue()) > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    // priority最小得为一级目录
    //    @Override
    //    public int compare(MetaPairs meta, MetaPairs other) {
    //        if (meta.getPriority() < other.getPriority()) {
    //            return -1;
    //        } else if (meta.getPriority() > other.getPriority()) {
    //            return 1;
    //        } else {
    //            if (meta.getValue().compareTo(other.getValue()) > 0) {
    //                return -1;
    //            } else {
    //                return 1;
    //            }
    //        }
    //    }
}
