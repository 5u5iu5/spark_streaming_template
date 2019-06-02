package com.adelpozo.streaming.streams.writer

import java.util.Properties

import com.adelpozo.streaming.config.AppConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.StringSerializer


object KafkaProducerLaziness {
  def apply[T](appConfig: AppConfig): KafkaProducerLaziness[T] = {
    require(appConfig != null)
    val functionToCreateProducer = () => {
      val producer: KafkaProducer[String, T] = new KafkaProducer[String, T](getProducerProperties(appConfig))
      sys.addShutdownHook(producer.close())
      producer
    }
    new KafkaProducerLaziness[T](functionToCreateProducer)
  }

  private def getProducerProperties(appConfig: AppConfig): Properties = {
    val props: Properties = new Properties()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.application.parameters("kafkaProducerServers"))
    props.put("schema.registry.url", appConfig.application.parameters("schemaRegistryURL"))
    props.put("auto.register.schemas", false: java.lang.Boolean)

    val kerberos = appConfig.application.parameters("kafkaWithKerberos").toBoolean

    kerberos match {
      case true =>
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name)
        props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka")
      case _ =>
    }

    props
  }
}

class KafkaProducerLaziness[T](createProducerFuntion: () => KafkaProducer[String, T]) extends Serializable {

  lazy val producer: KafkaProducer[String, T] = createProducerFuntion()

  def send(topic: String, value: T): Unit = {
    producer.send(new ProducerRecord(topic, value))
  }
}
