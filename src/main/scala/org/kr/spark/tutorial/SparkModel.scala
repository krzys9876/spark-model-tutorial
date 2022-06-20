package org.kr.spark.tutorial

import org.apache.spark.sql.functions.{coalesce, col, lit, max}
import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkModel {
  private def getJoinedInputWithBase(baseFile:String,inputFile:String)(implicit spark:SparkSession):DataFrame = {
    val base=FileLoader.loadCSV(baseFile,Schemas.base)
    val input=FileLoader.loadCSV(inputFile,Schemas.input)
    val inputPeriod=input.agg(max("input_period")).first().getAs[Long](0)
    val prevBase=base.filter(col("period").equalTo(inputPeriod-1))
    val joined=prevBase.join(input,List("sensor"),"full")
    joined.show()
    joined
  }

  def processSparkSQL(baseFile:String, inputFile:String)(implicit spark:SparkSession):DataFrame= {
    val joined=getJoinedInputWithBase(baseFile,inputFile)

    joined.createOrReplaceTempView("joined")

    val preOutput=spark.sql(
      """
        |select
        | sensor,
        | input_period as period,
        | input_temp as temp,
        | next_temp_extrapl as temp_extrapl,
        | input_period+1 as next_period,
        | period_count+1 as period_count,
        | sum_period_temp+input_period*input_temp as sum_period_temp,
        | sum_period+input_period as sum_period,
        | sum_temp+input_temp as sum_temp,
        | sum_period_sqr+input_period*input_period as sum_period_sqr
        | from joined
        | """.stripMargin)

    preOutput.createOrReplaceTempView("pre_output")

    val preOutput2=spark.sql(
      """
        |select
        | sensor,period,temp,temp_extrapl,next_period,period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr,
        | (period_count * sum_period_temp - sum_period * sum_temp)/
        | (period_count * sum_period_sqr - sum_period * sum_period) as lin_reg_a,
        | (sum_temp * sum_period_sqr - sum_period * sum_period_temp)/
        | (period_count * sum_period_sqr - sum_period * sum_period) as lin_reg_b
        | from pre_output
      """.stripMargin
    )

    preOutput2.createOrReplaceTempView("pre_output2")

    val output=spark.sql(
      """
        |select
        | sensor,period,temp,temp_extrapl,next_period,
        | next_period * lin_reg_a + lin_reg_b as next_temp_extrapl,
        | lin_reg_a,lin_reg_b,
        | period_count,sum_period_temp,sum_period,sum_temp,sum_period_sqr
        | from pre_output2
      """.stripMargin
    )

    output.show()
    output

  }

  def processSparkFunctions(baseFile:String, inputFile:String)(implicit spark:SparkSession):DataFrame= {
    val joined=getJoinedInputWithBase(baseFile,inputFile)

    val output=
      joined
        .withColumn("period",col("input_period"))
        .withColumn("temp",col("input_temp"))
        .withColumn("temp_extrapl",col("next_temp_extrapl"))
        .withColumn("next_period",col("input_period")+1)
        .withColumn("period_count",coalesce(col("period_count"),lit(0))+1)
        .withColumn("sum_period_temp",coalesce(col("sum_period_temp"),lit(0))+(col("input_period")*col("input_temp")))
        .withColumn("sum_period",coalesce(col("sum_period"),lit(0))+col("input_period"))
        .withColumn("sum_temp",coalesce(col("sum_temp"),lit(0))+col("input_temp"))
        .withColumn("sum_period_sqr",coalesce(col("sum_period_sqr"),lit(0))+col("input_period")*col("input_period"))
        .withColumn("lin_reg_a",
          (col("period_count") * col("sum_period_temp") - col("sum_period") * col("sum_temp"))/
            (col("period_count") * col("sum_period_sqr") - col("sum_period") * col("sum_period")))
        .withColumn("lin_reg_b",
          (col("sum_temp") * col("sum_period_sqr") - col("sum_period") * col("sum_period_temp"))/
            (col("period_count") * col("sum_period_sqr") - col("sum_period") * col("sum_period")))
        .withColumn("next_temp_extrapl",col("lin_reg_a") * col("next_period") + col("lin_reg_b"))
        .drop("input_period","input_temp")

    output.show()
    output
  }

  def processSparkScala(baseFile:String, inputFile:String)(implicit spark:SparkSession):DataFrame= {
    val joined=getJoinedInputWithBase(baseFile,inputFile)

    import spark.implicits._
    val output=
      joined
        .as[BaseWithInput]
        .map(Base(_))
        .toDF()

    output.show()
    output
  }
}

case class Base(
                 sensor: Long,
                 period: Long,
                 temp: Double,
                 temp_extrapl: Option[Double],
                 next_period: Long,
                 next_temp_extrapl: Option[Double],
                 lin_reg_a: Option[Double],
                 lin_reg_b: Option[Double],
                 period_count: Long,
                 sum_period_temp: Double,
                 sum_period: Long,
                 sum_temp: Double,
                 sum_period_sqr: Long
               )

object Base {
  def apply(joined:BaseWithInput):Base = {
    val nextPeriod=joined.input_period+1
    val periodCount=joined.period_count.getOrElse(0L) + 1
    val sumPeriodTemp=joined.sum_period_temp.getOrElse(0.0)+joined.input_period*joined.input_temp
    val sumPeriod=joined.sum_period.getOrElse(0L)+joined.input_period
    val sumTemp=joined.sum_temp.getOrElse(0.0)+joined.input_temp
    val sumPeriodSqr=joined.sum_period_sqr.getOrElse(0L) + joined.input_period * joined.input_period

    val denominator=periodCount * sumPeriodSqr - sumPeriod * sumPeriod

    val (linRegA,linRegB,nextTempExtrapl) = denominator match {
      case 0.0 => (None, None, None)
      case denominator =>
        val a=(periodCount * sumPeriodTemp - sumPeriod * sumTemp)/denominator
        val b=(sumTemp * sumPeriodSqr - sumPeriod * sumPeriodTemp)/denominator
        (Some(a),Some(b),Some(nextPeriod*a+b))
    }

    new Base(joined.sensor,joined.input_period,joined.input_temp,joined.next_temp_extrapl,
      nextPeriod,nextTempExtrapl,linRegA,linRegB,
      periodCount,sumPeriodTemp,sumPeriod,sumTemp,sumPeriodSqr
    )
  }
}

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

