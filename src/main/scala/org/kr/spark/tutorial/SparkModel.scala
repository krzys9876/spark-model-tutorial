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

    output.show()
    output

/*    val linReg=
      joined
        .select("input_period","input_temp").collect().toList
        .map(row=>(row(0).asInstanceOf[Long].toDouble,row(1).asInstanceOf[Double])) ++
        base
          .select("period","temp").collect().toList
          .map(row=>(row(0).asInstanceOf[Long].toDouble,row(1).asInstanceOf[Double]))

    val count=linReg.length
    val sumPeriodTemp=linReg.foldLeft(0.0)((s,e)=>s + e._1*e._2)
    val sumPeriod=linReg.foldLeft(0.0)((s,e)=>s + e._1)
    val sumTemp=linReg.foldLeft(0.0)((s,e)=>s + e._2)
    val sumPeriodSqr=linReg.foldLeft(0.0)((s,e)=>s + e._1*e._1)

    val linRegA=(count * sumPeriodTemp-sumPeriod * sumTemp)/(count * sumPeriodSqr-sumPeriod * sumPeriod)
    val linRegB=(sumTemp * sumPeriodSqr-sumPeriod * sumPeriodTemp)/(count * sumPeriodSqr-sumPeriod * sumPeriod)

    println(f"A=$linRegA B=$linRegB nX=${5*linRegA+linRegB}")

    val rows=Seq((1,4,20.787,19.1126666666667,5,21.626,1.34129999999999,14.9195,4,189.434,10.0,73.091,30.0))
    import spark.implicits._
    val columnNames=Schemas.base.map(_.name)
    spark.sparkContext.parallelize(rows).toDF(columnNames : _*)*/
  }
}
