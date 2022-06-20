package org.kr.spark.tutorial

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

object Schemas {
  val input: StructType = StructType(List(
    StructField("sensor", LongType),
    StructField("input_period", LongType),
    StructField("input_temp", DoubleType)
  ))

  val base: StructType = StructType(List(
    StructField("sensor", LongType),
    StructField("period", LongType),
    StructField("temp", DoubleType),
    StructField("temp_extrapl", DoubleType),
    StructField("next_period", LongType),
    StructField("next_temp_extrapl", DoubleType),
    StructField("lin_reg_a", DoubleType),
    StructField("lin_reg_b", DoubleType),
    StructField("period_count", LongType),
    StructField("sum_period_temp", DoubleType),
    StructField("sum_period", LongType),
    StructField("sum_temp", DoubleType),
    StructField("sum_period_sqr", LongType)
  ))
}

case class Base(
                 sensor: Long,
                 period: Option[Long],
                 temp: Option[Double],
                 temp_extrapl: Option[Double],
                 next_period: Option[Long],
                 next_temp_extrapl: Option[Double],
                 lin_reg_a: Option[Double],
                 lin_reg_b: Option[Double],
                 period_count: Option[Long],
                 sum_period_temp: Option[Double],
                 sum_period: Option[Long],
                 sum_temp: Option[Double],
                 sum_period_sqr: Option[Long],
               )

case class BaseWithInput(
                          sensor: Long,
                          period: Option[Long],
                          temp: Option[Double],
                          temp_extrapl: Option[Double],
                          next_period: Option[Long],
                          next_temp_extrapl: Option[Double],
                          lin_reg_a: Option[Double],
                          lin_reg_b: Option[Double],
                          period_count: Option[Long],
                          sum_period_temp: Option[Double],
                          sum_period: Option[Long],
                          sum_temp: Option[Double],
                          sum_period_sqr: Option[Long],
                          input_period: Long,
                          input_temp: Double
                        )
