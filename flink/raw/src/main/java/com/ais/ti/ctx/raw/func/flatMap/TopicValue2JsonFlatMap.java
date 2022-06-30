package com.ais.ti.ctx.raw.func.flatMap;

import com.ais.ti.ctx.raw.entities.*;
import com.ais.ti.ctx.raw.utils.JsonUtils;
import com.ais.ti.ctx.raw.utils.PathUtils;
import com.alibaba.fastjson.JSON;
//import com.asiainfo_sec.datalake.rawdata.entities.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class TopicValue2JsonFlatMap implements FlatMapFunction<TopicValue, SinkPathData> {
    private DataPairs dataPairs = new DataPairs();

    public TopicValue2JsonFlatMap() {
    }

    private String toJson(Object data, Object extraData) {
        dataPairs.setData(data);
        dataPairs.setExtraData(extraData);
        return JSON.toJSONString(dataPairs);
    }

    @Override
    public void flatMap(TopicValue topicValue, Collector<SinkPathData> out) throws Exception {
        String topic = topicValue.getTopic();
        String value = topicValue.getValue();
        KafkaFieldStateData kafkaFieldStateData = JsonUtils.parsekafkaValue(value);
        if (kafkaFieldStateData.getStatus()) {
            String meta = kafkaFieldStateData.getMeta();
            String extraData = kafkaFieldStateData.getExtraData();
            String data = kafkaFieldStateData.getData();

            MetaPathStateData metaPathStateData = JsonUtils.parseMetaPath(meta);
            if (metaPathStateData.getStatus()) {
                String subPath = metaPathStateData.getSubPath();
                out.collect(new SinkPathData(subPath, topic, toJson(data, extraData)));
                out.collect(new SinkPathData(PathUtils.getMetaSubPath(), topic, toJson(meta, "")));
            } else {
                out.collect(new SinkPathData(PathUtils.getErrSubPath(topic), topic, toJson(value, "")));
            }
        } else {
            out.collect(new SinkPathData(PathUtils.getErrSubPath(topic), topic, toJson(value, "")));
        }
    }
}
