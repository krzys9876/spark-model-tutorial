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

  import FileLoader._
  for(period <- List.range(1,12)) {
    logger.info(f"Calculating period: $period")
    SparkModel
      .processSparkScala(f"data/base_${period-1}",f"data/input_$period.csv")
      .saveCSV(f"data/base_$period")
  }

  spark.close()

  logger.info("END")
}
