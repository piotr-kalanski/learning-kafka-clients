package com.datawizards.kafka.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class WriteSyncAvroMessage {

    private static final String TOPIC = "test_avro";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put("client.id", "write-avro-message");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("acks", "all");

        String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                "\"name\": \"page_visit\"," +
                "\"fields\": [" +
                "{\"name\": \"time\", \"type\": \"long\"}," +
                "{\"name\": \"site\", \"type\": \"string\"}," +
                "{\"name\": \"ip\", \"type\": \"string\"}" +
                "]}";

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(config);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < 100; nEvents++) {
            long runtime = new Date().getTime();
            String site = "www.example.com";
            String ip = "192.168.2." + rnd.nextInt(255);

            GenericRecord page_visit = new GenericData.Record(schema);
            page_visit.put("time", runtime);
            page_visit.put("site", site);
            page_visit.put("ip", ip);

            ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, GenericRecord>(TOPIC, ip, page_visit);
            producer.send(data);
        }

        producer.close();
    }
}
