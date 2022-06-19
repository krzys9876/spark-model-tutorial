package org.kr.spark.tutorial.test

object TestUtils {
  def roundAny(value:Any,decimals:Int):Double = {
    val factor:Double=math.pow(10,decimals)
    math.rint(value.asInstanceOf[Double]*factor)/factor
  }

  def equalsRounded(expected:Double,actual:Any,decimals:Int):Boolean = {
    expected==roundAny(actual,decimals)
  }
}
