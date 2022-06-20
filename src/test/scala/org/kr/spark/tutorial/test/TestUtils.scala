package org.kr.spark.tutorial.test

import org.apache.spark.sql.{DataFrame, Row}
import org.kr.spark.tutorial.Base

object TestUtils {
  def roundAny(value:Any,decimals:Int):Double = {
    val valueAsDouble=value match {
      case v:Double=>v
      case v:Long=>v.toDouble
    }
    val factor:Double=math.pow(10,decimals)
    math.rint(valueAsDouble*factor)/factor
  }

  def equalsRounded(expected:Double,actual:Any,decimals:Int):Boolean = {
    expected==roundAny(actual,decimals)
  }

  def baseEquals(expected:Base, actual:Base, decimals:Int):Boolean =
    expected.sensor==actual.sensor &&
    expected.period==actual.period &&
    equalsRounded(expected.temp,actual.temp,decimals) &&
    equalsRounded(expected.temp_extrapl.getOrElse(0.0),actual.temp_extrapl.getOrElse(0.0),decimals) &&
      expected.next_period==actual.next_period &&
      equalsRounded(expected.next_temp_extrapl.getOrElse(0.0),actual.next_temp_extrapl.getOrElse(0.0),decimals) &&
      equalsRounded(expected.lin_reg_a.getOrElse(0.0),actual.lin_reg_a.getOrElse(0.0),decimals) &&
      equalsRounded(expected.lin_reg_b.getOrElse(0.0),actual.lin_reg_b.getOrElse(0.0),decimals) &&
      expected.period_count==actual.period_count &&
      equalsRounded(expected.sum_period_temp,actual.sum_period_temp,decimals) &&
      expected.sum_period==actual.sum_period &&
      equalsRounded(expected.sum_temp,actual.sum_temp,decimals) &&
      expected.sum_period_sqr==actual.sum_period_sqr

  def equalsRounded(expected:Array[Double],actual:Row,decimals:Int):Boolean = {
    val expectedRounded=expected.map(roundAny(_,decimals))
    val actualRounded=actual.toSeq.map(roundAny(_,decimals))
    expectedRounded.sameElements(actualRounded)
  }

  def getOutputRow(df:DataFrame,sensor:Long,period:Long):Row={
    df
      .filter(f"sensor = $sensor and period = $period")
      .collect()(0)
  }
}
