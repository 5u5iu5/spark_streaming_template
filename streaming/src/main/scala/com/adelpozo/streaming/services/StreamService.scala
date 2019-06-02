package com.adelpozo.streaming.services

import com.adelpozo.streaming.entities.TablesInfo.T_Data._
import com.adelpozo.streaming.events.InputEvent
import com.adelpozo.streaming.events.model.Info
import com.adelpozo.streaming.streams.writer.KafkaProducerLaziness
import com.adelpozo.streaming.utils.KuduDataSource
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


object StreamService {

  @transient
  lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  def apply(kuduDataSource: KuduDataSource,
            kafkaProducerLaziness: KafkaProducerLaziness[Info],
            input: InputEvent): StreamService = {

    new StreamService(EntityService.getEntity(kuduDataSource, input), kafkaProducerLaziness)
  }
}

case class StreamService(infoList: List[InfoTable], kafkaProducerLaziness: KafkaProducerLaziness[Info]) extends Serializable {

  import StreamService.logger

  def buildByDataframeAndSelfProduce(topic: String): Unit = {
    infoList.foreach(info => {
      val avroInfoObject = Info(info.value1, info.value2, info.value3)
      logger.info(s"Publishing message ${avroInfoObject.toString} to topic $topic")
      kafkaProducerLaziness.send(topic, avroInfoObject)
      logger.info("Publish Done!")
    })
  }
}
