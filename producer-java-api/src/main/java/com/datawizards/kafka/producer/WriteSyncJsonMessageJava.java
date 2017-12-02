package com.datawizards.kafka.producer;

import com.datawizards.kafka.model.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WriteSyncJsonMessageJava {

    private static final String TOPIC = "test_json";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put("client.id", "write-json-message");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        config.put("acks", "all");

        KafkaProducer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(config);
        ObjectMapper objectMapper = new ObjectMapper();

        for(int i=0;i<100;++i) {
            try {
                JsonNode jsonNode = objectMapper.valueToTree(new Person(i, "person_" + i));
                ProducerRecord<String, JsonNode> record = new ProducerRecord<String, JsonNode>(TOPIC, i + "", jsonNode);
                producer.send(record).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
