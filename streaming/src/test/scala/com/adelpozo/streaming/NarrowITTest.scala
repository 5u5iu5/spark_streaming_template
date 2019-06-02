package com.adelpozo.streaming

import java.text.SimpleDateFormat

import com.adelpozo.streaming.config.AppConfig
import com.adelpozo.streaming.entities.TablesInfo.T_Data
import com.adelpozo.streaming.events.model.Info
import com.adelpozo.streaming.process.ConsumeAndProduceProcess
import com.adelpozo.streaming.tags.NarrowTestsSuite
import com.adelpozo.streaming.utils.docker.{DockerKafkaService, DockerKuduService}
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.scalatest.DockerTestKit
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkContextForTests
import org.apache.spark.streaming.{Clock, StreamingContext}
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import com.adelpozo.streaming.services.KafkaService._
import com.adelpozo.streaming.services.KuduService._
import com.adelpozo.streaming.services.SchemaRegistryService._

@NarrowTestsSuite
class NarrowITTest extends FlatSpec with Matchers with Eventually with SparkContextForTests
  with DockerTestKit with DockerKitDockerJava with DockerKuduService with DockerKafkaService {

  implicit val pc = PatienceConfig(Span(20, Seconds), Span(1, Second))

  implicit var ssc: StreamingContext = _

  val configPath: String = getClass.getClassLoader.getResource(".").getPath

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  override def beforeAll(): Unit = {
    super.beforeAll()
    initKudu
    initKafka
    initSchemaRegistry
  }

  override def afterAll(): Unit = {
    ssc.stop(stopSparkContext = true, stopGracefully = false)
    closeKudu
    closeKafka
    closeSchemaRegistry
    super.afterAll()
  }

  "kudu tables" should "be created" in {
    kuduClient.getTablesList.getTablesList.size() shouldBe 1
    kuduClient.tableExists(T_Data.TABLENAME) shouldBe true
    countRecords(T_Data.TABLENAME) shouldBe 4
  }

  "kafka topics" should "be created" in {
    AdminUtils.topicExists(ZkUtils.apply("localhost:2181", 30000, 30000, false), topicToConsume) shouldBe true
    AdminUtils.topicExists(ZkUtils.apply("localhost:2181", 30000, 30000, false), topicToPublish) shouldBe true
  }

  "Process" should "publish tlog tickets in kafka" in {

    val appConf = AppConfig(s"$configPath/application.conf")

    val batchDuration = org.apache.spark.streaming.Seconds(appConf.application.parameters("sparkBatchDuration").toLong)

    ssc = new StreamingContext(sparkSession.sparkContext, batchDuration)

    val fixedClock = Clock.getFixedClock(ssc)

    ConsumeAndProduceProcess.processPipeline(appConf, ssc, sparkSession)

    var records: ConsumerRecords[String, Info] = kafkaConsumer.poll(100)
    records.count() shouldBe 0


    println("### Streaming Should fail ###")
    kafkaProducer.send(new ProducerRecord[String, String](topicToConsume, null, "123420190101"))

    eventually(timeout(Span(15, Seconds)), interval(Span(1, Seconds))) {
      fixedClock.addTime(batchDuration)
      records = kafkaConsumer.poll(100)
      records.count() shouldBe 0
    }

    println("### Streaming Should produce ALVARO  ###")
    kafkaProducer.send(new ProducerRecord[String, String](topicToConsume, null, "000120190501"))

    eventually(timeout(Span(15, Seconds)), interval(Span(1, Seconds))) {
      fixedClock.addTime(batchDuration)
      records = kafkaConsumer.poll(100)
      records.count() shouldBe 1
    }

    val record1: ConsumerRecord[String, Info] = records.iterator().next()
    record1.offset() shouldBe 0
    record1.value() should be(Info("ALVARO", "POZO", "CUENCA"))

    println("### Streaming Should produce ARYA  ###")
    kafkaProducer.send(new ProducerRecord[String, String](topicToConsume, null, "000220190502"))

    eventually(timeout(Span(15, Seconds)), interval(Span(1, Seconds))) {
      fixedClock.addTime(batchDuration)
      records = kafkaConsumer.poll(100)
      records.count() shouldBe 1
    }

    val record2: ConsumerRecord[String, Info] = records.iterator().next()
    record2.offset() shouldBe 1
    record2.value() should be(Info("ARYA", "POZO", "CUENCA"))

    println("### Streaming Should produce ARYA  ###")
    kafkaProducer.send(new ProducerRecord[String, String](topicToConsume, null, "000320190503"))

    eventually(timeout(Span(15, Seconds)), interval(Span(1, Seconds))) {
      fixedClock.addTime(batchDuration)
      records = kafkaConsumer.poll(100)
      records.count() shouldBe 1
    }

    val record3: ConsumerRecord[String, Info] = records.iterator().next()
    record3.offset() shouldBe 2
    record3.value() should be(Info("JON", "POZO", "CUENCA"))

    println("### Streaming Should produce LAURA  ###")
    kafkaProducer.send(new ProducerRecord[String, String](topicToConsume, null, "000420190504"))

    eventually(timeout(Span(15, Seconds)), interval(Span(1, Seconds))) {
      fixedClock.addTime(batchDuration)
      records = kafkaConsumer.poll(100)
      records.count() shouldBe 1
    }

    val record4: ConsumerRecord[String, Info] = records.iterator().next()
    record4.offset() shouldBe 3
    record4.value() should be(Info("LAURA", "POZO", "CUENCA"))
  }

}