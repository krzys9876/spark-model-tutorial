package org.kr.spark.tutorial

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkModel {
  def process(baseFile:String,inputFile:String)(implicit spark:SparkSession):DataFrame= {
    val rows=Seq((1,4,20.787,19.1126666666667,5,21.626,1.34129999999999,14.9195))
    import spark.implicits._
    val columnNames=Schemas.base.map(_.name)
    spark.sparkContext.parallelize(rows).toDF(columnNames : _*)
  }
}
