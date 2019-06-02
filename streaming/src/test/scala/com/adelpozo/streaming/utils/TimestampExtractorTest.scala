package com.adelpozo.streaming.utils

import java.util.TimeZone

import org.scalatest._


class TimestampExtractorTest extends FlatSpec with Matchers {

  val extractor = new TimestampExtractor

  "getTimestamp" should "return microseconds" in {
    val time = System.currentTimeMillis
    extractor.getTimestamp(time) shouldBe time * 1000L
  }

  "getTimestamp" should "parse string with format: yyyy-MM-dd HH:mm:ss" in {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("Europe/Madrid"))
    val time1 = sdf.parse("2018-12-19 14:00:00").getTime
    extractor.getTimestamp("2018-12-19 14:00:00") shouldBe time1 * 1000L
    val time2 = sdf.parse("2018-08-19 14:00:00").getTime
    extractor.getTimestamp("2018-08-19 14:00:00") shouldBe time2 * 1000L
  }

  "getTimestamp" should "parse string with format: yyyy-MM-dd" in {
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
    sdf.setTimeZone(TimeZone.getTimeZone("Europe/Madrid"))
    val time1 = sdf.parse("2018-12-19").getTime
    extractor.getTimestamp("2018-12-19") shouldBe time1 * 1000L
    val time2 = sdf.parse("2018-08-19").getTime
    extractor.getTimestamp("2018-08-19") shouldBe time2 * 1000L
  }

  "getTimestamp" should "fail with invalid formats" in {
    an [IllegalArgumentException] should be thrownBy extractor.getTimestamp("19/12/2018 23:00:00")
    an [IllegalArgumentException] should be thrownBy extractor.getTimestamp("19/12/2018")
    an [IllegalArgumentException] should be thrownBy extractor.getTimestamp("2018.12.19")
    an [IllegalArgumentException] should be thrownBy extractor.getTimestamp("")
  }

}
