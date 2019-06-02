package org.apache.spark

import org.apache.spark.sql.SparkSession

trait SparkContextForTests {

  val sparkConf: SparkConf = new SparkConf().set("spark.driver.allowMultipleContexts", "true").set("spark.streaming.clock", "org.apache.spark.FixedClock")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).appName("bdsales-export-t-online").master("local[2]").getOrCreate()

}
