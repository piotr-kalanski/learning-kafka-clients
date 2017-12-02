package com.datawizards.kafka.producer

import java.util.Properties
import java.util.concurrent.ExecutionException

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object WriteSyncStringMessageScala extends App {
  val TOPIC = "test_string"

  val config = new Properties
  config.put("client.id", "write-string-message")
  config.put("bootstrap.servers", "localhost:9092")
  config.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  config.put("acks", "all")

  val producer = new KafkaProducer[Integer, String](config)
  val objectMapper = new ObjectMapper

  for(i <- 1 to 100) {
    try
      producer.send(new ProducerRecord[Integer, String](TOPIC, i, "message_" + i)).get
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
      case e: ExecutionException =>
        e.printStackTrace()
    }
  }
}
