package com.datawizards.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class ReadStringMessage {

    public static final String TOPIC = "test_string";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put("group.id", "read-string-message");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(config);

        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(100);
                for (ConsumerRecord<Integer, String> record : records)
                {
                    System.out.println(record.topic() + " | " + record.partition() + " | " + record.offset());
                    System.out.println(record.key() + "->" + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
