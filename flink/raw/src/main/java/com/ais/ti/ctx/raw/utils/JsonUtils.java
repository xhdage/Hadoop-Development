package com.ais.ti.ctx.raw.utils;

import com.ais.ti.ctx.raw.func.comparator.MetaParsComparator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.ais.ti.ctx.raw.constants.KafkaRawJsonFields;
import com.ais.ti.ctx.raw.constants.MetaJsonFields;
import com.ais.ti.ctx.raw.entities.KafkaFieldStateData;
import com.ais.ti.ctx.raw.entities.MetaPairs;
import com.ais.ti.ctx.raw.entities.MetaPathStateData;

import java.util.LinkedList;
import java.util.Set;
import java.util.stream.Collectors;

public class JsonUtils {

    /**
     * 解析kafka的value字段值。
     *
     * @param kafkaValue kafka string value。
     * @return KafkaFieldData。
     */
    public static KafkaFieldStateData parsekafkaValue(String kafkaValue) {
        JSONObject jsonObject;
        try {
            jsonObject = JSON.parseObject(kafkaValue);
        } catch (JSONException ex) {
            return new KafkaFieldStateData(false);
        }
        if (jsonObject == null) {
            return new KafkaFieldStateData(false);
        }
        String rawData;
        String meta;
        String extraData;
        try {
            rawData = jsonObject.getObject(KafkaRawJsonFields.DATA_FIELD, Object.class).toString();
            meta = jsonObject.getObject(KafkaRawJsonFields.META_FIELD, Object.class).toString();
            extraData = jsonObject.getObject(KafkaRawJsonFields.EXTRA_FIELD, Object.class).toString();
        } catch (JSONException | NullPointerException ex) {
            return new KafkaFieldStateData(false);
        }
        return new KafkaFieldStateData(true, rawData, meta, extraData);
    }

    /**
     * 根据meta的内容，解析路径。
     *
     * @param meta kafka value data中的extra字段。
     * @return MetaPathStateData。
     */
    public static MetaPathStateData parseMetaPath(String meta) {
        JSONObject metaJson;
        try {
            metaJson = JSON.parseObject(meta);
        } catch (JSONException ex) {
            return new MetaPathStateData(false);
        }
        if (metaJson == null) {
            return new MetaPathStateData(false);
        }
        Set<String> keys = metaJson.keySet();
        LinkedList<MetaPairs> metas = new LinkedList<>();
        try {
            for (String key : keys) {
                JSONObject jsonObject = metaJson.getJSONObject(key);
                int priority =
                        Integer.parseInt(
                                jsonObject.getObject(MetaJsonFields.PRIORITY, Object.class).toString());
                String value = jsonObject.getObject(MetaJsonFields.VALUE, Object.class).toString();
                metas.add(new MetaPairs(priority, key + "=" + value));
            }
        } catch (NumberFormatException | NullPointerException ex) {
            return new MetaPathStateData(false);
        }
        metas.sort(new MetaParsComparator());
        String path = metas.stream().map(MetaPairs::getValue).collect(Collectors.joining("/"));
        return new MetaPathStateData(true, path);
    }
}
