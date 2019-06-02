package com.adelpozo.streaming.config

import java.io.File

import com.adelpozo.streaming.arguments.Args
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.{CamelCase, ConfigFieldMapping, ProductHint, loadConfigOrThrow}

case class AppConfig(application: Application)

case class Application(process: List[Process], topic: List[Topic], parameters: Map[String, String])

case class Process(name: String, description: String)

case class Topic(name: String, typeOf: String, description: String, schema: String, max_message_bytes: Int, format: String)

object AppConfig {

  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def fromConfig(conf: Config = ConfigFactory.load()): AppConfig = loadConfigOrThrow[AppConfig](conf)

  def apply(path: String): AppConfig = fromConfig(ConfigFactory.parseFile(new File(path)))

  def getConfiguration(args: Args) = AppConfig(s"${args.configPath}/application.conf")

}
