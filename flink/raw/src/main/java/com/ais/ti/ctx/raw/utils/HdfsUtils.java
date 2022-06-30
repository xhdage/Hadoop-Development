package com.ais.ti.ctx.raw.utils;

import org.apache.hadoop.conf.Configuration;

public class HdfsUtils {
    public static Configuration getConf() {
        Configuration config = new Configuration();
        config.setBoolean("dfs.support.append", true);
        config.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
        config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        return config;
    }
}
