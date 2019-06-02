package com.adelpozo.streaming.streams.reader

import com.adelpozo.streaming.config.AppConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

class KafkaStreamReader(appConfig: AppConfig, ssc: StreamingContext) {

  def readStream(): InputDStream[ConsumerRecord[String, String]] = {

    val servers = appConfig.application.parameters("kafkaServers")
    val group = appConfig.application.parameters("kafkaConsumerGroup")
    val topics = appConfig.application.topic.filter(_.typeOf == "suscribe").map(_.name)
    val kerberos = appConfig.application.parameters("kafkaWithKerberos").toBoolean

    val commonParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> servers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    val additionalSecuredParams = if (kerberos) {
      Map(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.SASL_PLAINTEXT.name,
        SaslConfigs.SASL_KERBEROS_SERVICE_NAME -> "kafka"
      )
    } else {
      Map.empty
    }

    val params = commonParams ++ additionalSecuredParams

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, params)
    )

  }

}
