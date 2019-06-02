package com.adelpozo.streaming.process

import java.util.Date

import com.adelpozo.streaming.arguments.Args
import com.adelpozo.streaming.config.AppConfig
import com.adelpozo.streaming.events.model.Info
import com.adelpozo.streaming.events.{Empty, FormatterFromInLine, InputEvent}
import com.adelpozo.streaming.services.StreamService
import com.adelpozo.streaming.streams.reader.KafkaStreamReader
import com.adelpozo.streaming.streams.writer.KafkaProducerLaziness
import com.adelpozo.streaming.utils.{KuduDataSource, SparkCommons}
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

case class Record(id: String, timestamp: Long, input: InputEvent)

case class ConstructStream(kuduDataSource: KuduDataSource, stream: InputDStream[ConsumerRecord[String, String]], kafkaLazyPublisher: Broadcast[KafkaProducerLaziness[Info]])

object ConsumeAndProduceProcess extends SparkCommons with Serializable {

  type TransCodMap = Map[String, List[mutable.Map[String, Any]]]

  @transient
  lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  def apply(arg: Args, appConfig: AppConfig): Unit = {
    val (interval: Long, spark: SparkSession) = createSparkSession(arg, appConfig)
    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(interval))
    processPipeline(appConfig, ssc, spark)
    setupGracefulStop(appConfig, ssc)
  }


  def processPipeline(appConfig: AppConfig, ssc: StreamingContext, sparkSession: SparkSession): Unit = {

    val ConstructStream(kuduDataSource: KuduDataSource,
    stream: InputDStream[ConsumerRecord[String, String]],
    kafkaLazyPublisher: Broadcast[KafkaProducerLaziness[Info]]) = constructStream(appConfig, ssc, sparkSession)

    stream.foreachRDD {
      rddFromKafkaApplyingLogic(_, stream,
        kuduDataSource,
        sparkSession,
        appConfig,
        kafkaLazyPublisher)
    }

    sys.ShutdownHookThread {
      logger.info("Shutdown hook called ...")
      ssc.stop(stopSparkContext = true, stopGracefully = true)
      logger.info("Application stopped!")
    }

    ssc.start

    logger.info("StreamingContext started ...")
  }

  private def constructStream(appConfig: AppConfig, ssc: StreamingContext, sparkSession: SparkSession) = {
    logger.info("Processing pipeline ...")
    val kafkaStreamReader = new KafkaStreamReader(appConfig, ssc)

    logger.info("Creating Kudu Context ...")
    val kuduDataSource = KuduDataSource(sparkSession, appConfig.application.parameters("kuduHost"))

    logger.info("Creating Kafka stream ...")
    val stream: InputDStream[ConsumerRecord[String, String]] = kafkaStreamReader.readStream()

    logger.info("Creating Kafka Lazyness Producer ...")
    val kafkaLazyPublisher: Broadcast[KafkaProducerLaziness[Info]] =
      sparkSession.sparkContext.broadcast(KafkaProducerLaziness[Info](appConfig))
    ConstructStream(kuduDataSource, stream, kafkaLazyPublisher)
  }

  private def rddFromKafkaApplyingLogic(rdd: RDD[ConsumerRecord[String, String]],
                                        stream: InputDStream[ConsumerRecord[String, String]],
                                        kuduDataSource: KuduDataSource,
                                        sparkSession: SparkSession,
                                        appConfig: AppConfig,
                                        kafkaProducerLaziness: Broadcast[KafkaProducerLaziness[Info]]): Unit = {

    val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    val topic: String = appConfig.application.topic.filter(_.typeOf == "publish").map(_.name).head

    val dataSetInfo: Dataset[Record] = createDataSetFromEvent(rdd)(sparkSession)

    dataSetInfo.foreach(processingEventToProduceToKafka(_, topic, kuduDataSource, kafkaProducerLaziness))

    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

  }

  private def processingEventToProduceToKafka(record: Record, topic: String, kuduDataSource: KuduDataSource,
                                              kafkaProducerLaziness: Broadcast[KafkaProducerLaziness[Info]]): Unit = {
    try {
      val builderObject = StreamService(kuduDataSource, kafkaProducerLaziness.value, record.input)
      builderObject.buildByDataframeAndSelfProduce(topic)
    } catch {
      case re: RuntimeException =>
        logger.error(s"${record.id} (${new Date(record.timestamp)}) -> (${record.input}): Error after map event to object. Can not produce. ERROR: ${re.getMessage}")
    }
  }

  private def createDataSetFromEvent(rdd: RDD[ConsumerRecord[String, String]])(sparkSession: SparkSession): Dataset[Record] = {
    import sparkSession.implicits._
    convertEvent(rdd).toDS()
  }

  private def convertEvent(rdd: RDD[ConsumerRecord[String, String]]): RDD[Record] = {
    rdd.flatMap[Record](cr => {
      val id: String = s"${cr.topic}-${cr.partition}:${cr.offset}"
      parseConsumerRecord(cr) match {
        case Some(inputEvent) => List[Record](Record(id, cr.timestamp(), inputEvent))
        case _ =>
          logger.error(s"Record '$id' (${new Date(cr.timestamp)}) -> Error parsing: '${cr.value()}'")
          List.empty
      }
    })
  }

  private def parseConsumerRecord(consumerRecord: ConsumerRecord[String, String]): Option[InputEvent] = {
    FormatterFromInLine(consumerRecord.value()).formatterToInfo match {
      case inputEvent: InputEvent => Some(inputEvent)
      case _: Empty => Option.empty[InputEvent]
    }
  }

}
