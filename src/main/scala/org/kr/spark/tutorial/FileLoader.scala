package org.kr.spark.tutorial

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.util.Try

object FileLoader {
  val options=Map(
    "header" -> "false",
    "dateFormat" -> "yyyy-MM-dd",
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
    "delimiter" -> ",")

  def loadCSV(file:String,schema:StructType)(implicit spark:SparkSession):DataFrame =
    spark
      .read
      .options(options)
      .format("CSV")
      .schema(schema)
      .load(file)

  implicit class SaveCSVWrapper[T](df:DataFrame) {
    def saveCSV(file:String):DataFrame = {
      df.write
        .options(options)
        .format("CSV")
        .mode(SaveMode.Overwrite)
        .save(file)
      df
    }
  }
}
