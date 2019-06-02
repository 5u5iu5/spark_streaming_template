package com.adelpozo.streaming.arguments

import scopt.OptionParser

object ArgsParser extends OptionParser[Args]("KafkaToKudu") {

  def getArguments(args: Array[String]): Args = {
    ArgsParser.parse(args, Args()) match {
      case None => sys.exit(1)
      case Some(arguments) => arguments
    }
  }

  head("stock streaming", "1.x")

  opt[String]('f', "configPath") required() valueName "configPath" action { (value, config) =>
    config.copy(configPath = value)
  } text "Config file path"

  opt[String]('a', "appName") required() valueName "appName" action { (value, config) =>
    config.copy(appName = value)
  } text "Application name"

}

case class Args(configPath: String = "", appName: String = "")