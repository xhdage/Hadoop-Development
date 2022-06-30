package com.xhdage.hotitems_analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducerUtil {
    public static void main(String[] args) throws IOException {
        writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws IOException {
        // 1、kafka producer 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2、创建生产者
        KafkaProducer<String, String> kafkaProducer =new KafkaProducer<String, String>(properties);

        // 3、读取文件发送
        BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\开发\\Java\\bigdata\\flink\\UserBehaviorAnalysis\\HotitemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));

        String line;
        while ((line = bufferedReader.readLine()) != null){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();
    }
}
