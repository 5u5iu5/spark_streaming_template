package com.adelpozo.streaming.writer

import java.util.Properties

import com.adelpozo.streaming.config.{AppConfig, Application}
import com.adelpozo.streaming.events.model.Info
import com.adelpozo.streaming.streams.writer.KafkaProducerLaziness
import com.adelpozo.streaming.tags.UnitTestsSuite
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

@UnitTestsSuite
class KafkaProducerLazinessWrapSpec extends FlatSpec with GivenWhenThen with Matchers {


  "Kafka producer factory" should "to create a KafkaProducer with the specific config" in {
    val kafkaL: KafkaProducerLaziness[Info] = KafkaProducerLaziness(AppConfig(Application(List.empty, List.empty, Map.empty)))
    assert(kafkaL != null)
//    kafkaL.send("ticket-online_test", generateObjectToSend)
  }

  private def getProperties: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[KafkaAvroSerializer].getCanonicalName)
    props.put("schema.registry.url", "localhost:8081")
    props.put("auto.register.schemas", (false: java.lang.Boolean))
    props
  }

}
