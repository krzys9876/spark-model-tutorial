package org.kr.spark.tutorial.test

import org.apache.spark.sql.SparkSession
import org.kr.spark.tutorial.{FileLoader, Schemas}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path}

class SparkTest extends AnyFunSuite with GivenWhenThen with BeforeAndAfterAll {
  private val logger=LoggerFactory.getLogger(getClass)

  implicit val spark:SparkSession=SparkSession.builder()
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
    assert(Files.exists(Path.of("input_test01.csv")))
    When("input file is read by spark session to data frame")
    val df=FileLoader.loadCSV("input_test01.csv",Schemas.input)
    Then("data frame contains data from file")
    val values=df.collect()
    assertResult(1)(values(0)(0))
    assertResult(4)(values(0)(1))
    assertResult(20.787)(values(0)(2))
  }

  test("Base file should be properly read to data frame") {
    Given("base file and spark session exist")
    assert(Files.exists(Path.of("base_test01.csv")))
    When("base file is read by spark session to data frame")
    val df=FileLoader.loadCSV("base_test01.csv",Schemas.base)
    Then("data frame contains data from file")
    val values=df.collect()
    assertResult(16.132)(values(0)(2))
    assertResult(20.592)(values(1)(5))
    val v:Double=values(2)(7).asInstanceOf[Double]
    assertResult(15.756667)(math.rint(v*1000000)/1000000)
  }
}
