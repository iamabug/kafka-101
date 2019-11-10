package com.iamabug;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class BasicConsumer {
    public static void main(String[] args) {
        // 1. 创建KafkaConsumer对象
        KafkaConsumer consumer = getConsumer();
        // 2. 订阅topic集合或者使用正则表达式
        // consumer.subscribe(Collections.singletonList("test"));
        consumer.subscribe(Pattern.compile("iamabug-.*"));
        // 3. 进入poll循环
        try {
            while (true) {
                // 3.1 从broker获取数据
                // poll方法的参数是指最多等待缓冲区被填满的超时时间，100ms还没有满的话，有多少返回多少
                ConsumerRecords<String, String> records = consumer.poll(100);
                // 3.2 处理数据
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Topic = %s, Partition = %s, Offset = %d, Value = '%s', Timestamp = %d\n",
                            record.topic(), record.partition(), record.offset(), record.value(), record.timestamp());
                }
            }
        } finally {
            // 4. 关闭KafkaConsumer对象
            consumer.close();
        }
    }

    // 创建Kafka Consumer对象
    private static KafkaConsumer<String, String> getConsumer() {
        Properties props = new Properties();
        // 以下三个参数是必须的
        props.put("bootstrap.servers", "localhost:9092");
        // 和producer的serializer对应，需要指定deserializer
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // group.id虽然不是必须的，但在实际使用中，很少会不指定它
        // 在测试的时候可能需要经常更换这里group.id
        props.put("group.id", "test-group-2");
        return new KafkaConsumer<String, String>(props);
    }
}
