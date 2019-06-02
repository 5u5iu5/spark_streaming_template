package com.adelpozo.streaming.services

import java.util.Properties

import com.adelpozo.streaming.events.model.Info
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer

import collection.JavaConverters._


object KafkaService {

  var kafkaProducer: KafkaProducer[String, String] = _
  var kafkaConsumer: KafkaConsumer[String, Info] = _

  val topicToConsume = "topicToConsume"
  val topicToPublish = "topicToPublish"

  def closeKafka: Unit = {
    kafkaProducer.close()
    kafkaConsumer.close()
  }

  def initKafka: Unit = {
    AdminUtils.createTopic(ZkUtils.apply("localhost:2181", 30000, 30000, false), topicToConsume, 3, 1)
    AdminUtils.createTopic(ZkUtils.apply("localhost:2181", 30000, 30000, false), topicToPublish, 1, 1)

    val propsProducer = new Properties()
    propsProducer.put("bootstrap.servers", "localhost:9092")
    propsProducer.put("acks", "all")
    propsProducer.put("retries", Integer.valueOf(0))
    propsProducer.put("batch.size", Integer.valueOf(16384))
    propsProducer.put("linger.ms", Integer.valueOf(1))
    propsProducer.put("buffer.memory", Integer.valueOf(33554432))
    propsProducer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    propsProducer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    kafkaProducer = new KafkaProducer[String, String](propsProducer)

    val propsConsumer = new Properties()
    propsConsumer.put("bootstrap.servers", "localhost:9092")
    propsConsumer.put("group.id", "test-bis-gid")
    propsConsumer.put("enable.auto.commit", "true")
    propsConsumer.put("auto.commit.interval.ms", "1000")
    propsConsumer.put("session.timeout.ms", "30000")
    propsConsumer.put("auto.offset.reset", "earliest")
    propsConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    propsConsumer.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    propsConsumer.put("schema.registry.url", "http://localhost:8081")
    propsConsumer.put("specific.avro.reader", "true")

    kafkaConsumer = new KafkaConsumer[String, Info](propsConsumer)
    kafkaConsumer.subscribe(List(topicToPublish).asJava)
  }

}
