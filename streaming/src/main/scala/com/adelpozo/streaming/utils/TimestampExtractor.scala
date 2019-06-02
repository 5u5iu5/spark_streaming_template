package com.adelpozo.streaming.utils

import java.text.SimpleDateFormat
import java.util.TimeZone


class TimestampExtractor {

  /*
        "yyyy-MM-dd'T'HH:mm:ss'Z'",
        "yyyy-MM-dd'T'HH:mm:ssZ",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "MM/dd/yyyy HH:mm:ss",
        "MM/dd/yyyy'T'HH:mm:ss.SSS'Z'",
        "MM/dd/yyyy'T'HH:mm:ss.SSSZ",
        "MM/dd/yyyy'T'HH:mm:ss.SSS",
        "MM/dd/yyyy'T'HH:mm:ssZ",
        "MM/dd/yyyy'T'HH:mm:ss",
        "yyyy:MM:dd HH:mm:ss",
        "yyyy-MM-dd",
        "yyyyMMdd",
        "yyyy/MM/dd",
        "yyMMdd"
  */

  // yyyy-MM-dd HH:mm:ss
  val dateTimeRegex = """\d{4}-\d\d-\d\d \d\d:\d\d:\d\d""".r
  val dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  dateTimeFormatter.setTimeZone(TimeZone.getTimeZone("Europe/Madrid"))

  // yyyy-MM-dd
  val dateRegex = """\d{4}-\d\d-\d\d""".r
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  dateFormatter.setTimeZone(TimeZone.getTimeZone("Europe/Madrid"))

  def getTimestamp(value: Any) : Long = {
    val milliseconds = value match {
      case long: Long => long
      case string: String => string match {
        case dateTimeRegex() => dateTimeFormatter.parse(string).getTime
        case dateRegex() => dateFormatter.parse(string).getTime
        case _ => throw new IllegalArgumentException(s"String '${value}' cannot be converted to timestamp")
      }
      case _ => throw new IllegalArgumentException(s"Value '${value}' cannot be converted to timestamp")
    }

    // UNIXTIME_MICROS column, the long value provided should be the number
    // of microseconds between a given time and January 1, 1970 UTC.

    milliseconds * 1000L
  }

}
