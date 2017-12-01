package com.datawizards.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class WriteSyncStringMessage {

    public static final String TOPIC = "test_string";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put("client.id", "write-string-message");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "all");

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(config);

        for(int i=0;i<100;++i) {
            try {
                producer.send(
                        new ProducerRecord<Integer, String>(
                                TOPIC,
                                i,
                                "message_" + i
                        )
                ).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
