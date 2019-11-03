package com.iamabug;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class BasicProducer {

    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "iamabug", "大数据学徒");


        // 轮流注释下面三个方法
//        new BasicProducer().fireAndForget(record);
//        new BasicProducer().synchronousSend(record);
        new BasicProducer().asynchronousSend(record);
    }

    private void fireAndForget(ProducerRecord record) {
        try {
            producer.send(record);
            // 注释掉下面这行，数据不会发送到Kafka
            Thread.sleep(10);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void synchronousSend(ProducerRecord record) {
        try {
            RecordMetadata result = (RecordMetadata) producer.send(record).get();
            System.out.println("Topic: " + result.topic());
            System.out.println("Partition: " + result.partition());
            System.out.println("Offset: " + result.offset());
            System.out.println("Timestamp: " + result.timestamp());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void asynchronousSend(ProducerRecord record) {
        producer.send(record, new MyProducerCallback());
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class MyProducerCallback implements Callback {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (null != exception) {
               exception.printStackTrace();
            } else {
                System.out.println("Offset: " + metadata.offset());
            }
        }
    }
}
