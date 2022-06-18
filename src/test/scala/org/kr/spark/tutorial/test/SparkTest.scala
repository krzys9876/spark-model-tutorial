package org.kr.spark.tutorial.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType, LongType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

class SparkTest extends AnyFunSuite with GivenWhenThen with BeforeAndAfterAll {
  private val logger=LoggerFactory.getLogger(getClass)

  val spark:SparkSession=SparkSession.builder()
    .appName("spark-tutorial-test")
    .master("local[1]")
    .getOrCreate()

  override def afterAll(): Unit = spark.close()

  test("Spark session should be created successfully") {
    When("spark session is created")
    Then("spark application name is equal to test default")
    assertResult("spark-tutorial-test")(spark.conf.get("spark.app.name"))
  }

  test("Input file should be properly read to data frame") {
    Given("input file and spark session exist")
    When("input file is read by spark session to data frame")
    val df=spark.read
      .options(Map(
        "header" -> "false",
        "dateFormat" -> "yyyy-MM-dd",
        "timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
        "delimiter" -> ","))
      .format("CSV")
      .schema(StructType(List(
        StructField("sensor",LongType),
        StructField("period",LongType),
        StructField("temp",DoubleType)
      )))
      .load("input_test01.csv")
    Then("data frame contains data from file")
    val values=df.collect()
    assert(values(0)(0)==1)
    assert(values(0)(1)==1)
    assert(values(0)(2)==16.132)
  }
}
