package com.adelpozo.streaming.utils.docker

import java.util.Properties

import com.adelpozo.streaming.tags.NarrowTestsSuite
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest._
import org.scalatest.time._

import scala.collection.JavaConverters._

@NarrowTestsSuite
class KafkaServiceSpec extends FlatSpec with Matchers with DockerTestKit with DockerKitDockerJava
  with DockerKafkaService {

  implicit val pc = PatienceConfig(Span(20, Seconds), Span(1, Second))

  "kafka container" should "be ready" in {
    isContainerReady(kafkaContainer).futureValue shouldBe true
  }

  "kafka" should "be ready" in {
    val producer = buildProducer()
    val consumer = buildConsumer()

    val record1 = new ProducerRecord[String, String]("test", "key1", "value1")
    val record2 = new ProducerRecord[String, String]("test", "key2", "value2")

    producer.send(record1).get().offset() shouldBe 0
    producer.send(record2).get().offset() shouldBe 1

    consumer.subscribe(List("test").asJava)

    consumer.poll(100).count() + consumer.poll(100).count() shouldBe 2

    consumer.close()
    producer.close()
  }

  def buildProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", Integer.valueOf(0))
    props.put("batch.size", Integer.valueOf(16384))
    props.put("linger.ms", Integer.valueOf(1))
    props.put("buffer.memory", Integer.valueOf(33554432))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](props)
  }

  def buildConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test-gid")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    new KafkaConsumer[String, String](props)
  }

}
