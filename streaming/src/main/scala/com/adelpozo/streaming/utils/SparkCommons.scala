package com.adelpozo.streaming.utils

import com.adelpozo.streaming.arguments.Args
import com.adelpozo.streaming.config.AppConfig
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import com.adelpozo.streaming.Launcher.{fileSystem, logger}

trait SparkCommons {

  def createSparkSession(parsedArguments: Args, appConfig: AppConfig): (Long, SparkSession) = {
    val sparkMaster = appConfig.application.parameters("sparkMaster")
    val interval = appConfig.application.parameters("sparkBatchDuration").toLong
    val sparkConf: SparkConf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName(parsedArguments.appName)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    (interval, spark)
  }

  def setupGracefulStop(appConfig: AppConfig, ssc: StreamingContext): Unit = {
    val sparkGracefulStopFile = new Path(appConfig.application.parameters("gracefulShutdownFile"))
    val checkIntervalMillis = 10000

    fileSystem.create(sparkGracefulStopFile, true)

    var isStopped = false

    while (!isStopped) {
      try {
        isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
        if (!isStopped && isShutdownRequested(sparkGracefulStopFile)) {
          //This means file does not exist in path, so stop gracefully
          logger.info("Gracefully stopping Spark Streaming ...")
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          logger.info("Application stopped!")
        }
      } catch {
        case t: Throwable =>
          logger.error(s"Error processing streaming: ${t.getMessage}")
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          throw t // to exit with error condition
      }
    }
  }

  def isShutdownRequested(sparkGracefulStopFile: Path): Boolean = {
    try {
      !fileSystem.exists(sparkGracefulStopFile)
    } catch {
      case _: Throwable => false
    }
  }

}
