package com.datawizards.kafka.producer

import java.util.Properties
import java.util.concurrent.ExecutionException

import com.datawizards.kafka.model.Person
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.jackson.Serialization

object WriteSyncJsonMessageScala extends App {
  val TOPIC = "test_json"

  val config = new Properties
  config.put("client.id", "write-json-message")
  config.put("bootstrap.servers", "localhost:9092")
  config.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  config.put("acks", "all")

  val producer = new KafkaProducer[Integer, String](config)

  implicit val formats = DefaultFormats

  for(i <- 1 to 100) {
    try {
      val message = Serialization.write(Person(i, "p_" + i))
      val record = new ProducerRecord[Integer, String](TOPIC, i, message)
      producer.send(record).get
    }
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
      case e: ExecutionException =>
        e.printStackTrace()
    }
  }
}
