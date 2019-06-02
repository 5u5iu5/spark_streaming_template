package com.adelpozo.streaming.config

import org.scalatest.{FlatSpec, Matchers}

class AppConfigTest extends FlatSpec with Matchers {

  trait DefaultConf {

    final val defaultProcess: List[Process] =
      Process("process1","des_process1")::Nil

    final val defaultTopic: List[Topic] =
      Topic("topicToConsume","suscribe", "des_topicToConsume","xxx_stream",9999,"json")::
        Topic("topicToPublish","publish", "des_topicToPublish","xxx_stream",9999,"avro"):: Nil

    final val defaultParameters = Map(
      "sparkMaster" -> "local[2]",
      "kuduHost" -> "localhost",
      "kafkaConsumerGroup" -> "test-gid",
      "kafkaServers" -> "localhost:9092",
      "kafkaProducerServers" -> "localhost:9092",
      "kafkaWithKerberos" -> "false",
      "schemaRegistryURL" -> "http://localhost:8081",
      "sparkBatchDuration" -> "60",
      "kuduMaxNumberOfColumns" -> "8",
      "gracefulShutdownFile" -> "sparkStreaming.dontRemove.file",
      "auditUser" -> "test",
      "timeZone" -> "Europe/Madrid",
      "dateFormat" -> "yyyy-MM-dd HH:mm:ss"
    )

    final val defaultApplication =
      Application(
        defaultProcess,
        defaultTopic,
        defaultParameters
      )

    final val defaultConfig =
      AppConfig(
        defaultApplication
      )
  }

  "The config" should "be retreaved correctly a default config" in new DefaultConf {
    AppConfig(getClass.getClassLoader.getResource("application.conf").getPath) shouldBe defaultConfig
  }

}
