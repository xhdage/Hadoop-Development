package com.ais.ti.ctx.raw.entities;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TopicValueSchema implements KafkaDeserializationSchema<TopicValue> {
    private TopicValue data = null;

    @Override
    public boolean isEndOfStream(TopicValue nextElement) {
        return false;
    }

    @Override
    public TopicValue deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (data == null) {
            data = new TopicValue();
        }
        data.setTopic(record.topic());
        data.setValue(new String(record.value()));
        return data;
    }

    @Override
    public TypeInformation<TopicValue> getProducedType() {
        return TypeExtractor.getForClass(TopicValue.class);
    }
}
