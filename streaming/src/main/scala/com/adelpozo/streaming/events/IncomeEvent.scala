package com.adelpozo.streaming.events

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

sealed trait Events extends Serializable

case class InputEvent(cod: String, date: String) extends Events

class Empty extends Events

case class FormatterFromInLine(lineRaw: String) extends Events {

  @transient
  lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  def formatterToInfo: Events = {
    val inputevent = Try(
      InputEvent(
        getDataPos("cod", lineRaw, (0, 4)),
        getDataPos("date", lineRaw, (4, lineRaw.length))
      )
    )

    inputevent match {
      case Success(c) => InputEvent(c.cod, c.date.substring(0, 4) + "-" + c.date.substring(4, 6) + "-" + c.date.substring(6, 8))
      case Failure(_) => new Empty
    }
  }

  private def getDataPos(field: String, raw: String, pos: (Int, Int)): String = raw.substring(pos._1, pos._2)

}





