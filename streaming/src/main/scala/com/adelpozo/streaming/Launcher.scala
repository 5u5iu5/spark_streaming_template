package com.adelpozo.streaming

import com.adelpozo.streaming.arguments.Args
import com.adelpozo.streaming.arguments.ArgsParser.getArguments
import com.adelpozo.streaming.config.AppConfig
import com.adelpozo.streaming.config.AppConfig.getConfiguration
import com.adelpozo.streaming.process.ConsumeAndProduceProcess
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory


object Launcher {

  val logger = Logger(LoggerFactory.getLogger(getClass))

  val fileSystem: FileSystem = FileSystem.get(new Configuration())

  def main(args: Array[String]): Unit = {

    val parsedArguments: Args = getArguments(args)
    val appConfig: AppConfig = getConfiguration(parsedArguments)
    logger.info(s"Loaded configuration")
    logger.info(s"[config.name: ${appConfig.application.process}]")
    logger.info(s"[config.parameters: ${appConfig.application.parameters}]")

    ConsumeAndProduceProcess(parsedArguments, appConfig)

  }

}
