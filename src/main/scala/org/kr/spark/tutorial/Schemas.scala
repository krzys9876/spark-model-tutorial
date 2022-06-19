package org.kr.spark.tutorial

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

object Schemas {
  val input: StructType = StructType(List(
    StructField("sensor",LongType),
    StructField("period",LongType),
    StructField("temp",DoubleType)
  ))

  val base: StructType = StructType(List(
    StructField("sensor",LongType),
    StructField("period",LongType),
    StructField("temp",DoubleType),
    StructField("temp_extrapl",DoubleType),
    StructField("next_period",LongType),
    StructField("next_temp_extrapl",DoubleType),
    StructField("lin_reg_a",DoubleType),
    StructField("lin_reg_b",DoubleType)
  ))
}
