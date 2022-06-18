package org.kr.spark.tutorial

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main extends App {
  val logger=LoggerFactory.getLogger(getClass)

  logger.info("START")

  implicit val spark:SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("spark-model-tutorial")
      .getOrCreate()

  logger.info(spark.conf.getAll.mkString("|"))

  spark.close()

  logger.info("END")
}
