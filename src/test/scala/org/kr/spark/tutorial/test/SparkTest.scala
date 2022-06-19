package org.kr.spark.tutorial.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.kr.spark.tutorial.{FileLoader, Schemas, SparkModel}
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
    assert(TestUtils.equalsRounded(15.756667,values(2)(7),6))
  }

  test("Calculated base for next period should equal to reference data") {
    Given("base file exist")
    assert(Files.exists(Path.of("base_test01.csv")))
    And("input file exists")
    assert(Files.exists(Path.of("input_test01.csv")))
    When("current period data is processed")
    val df=SparkModel.process("base_test01.csv","input_test01.csv")
    Then("data frame contains data from file")
    val values=df
      .filter(
        col("period").equalTo(4)
          .and(col("sensor").equalTo(1)))
      .collect()
    assertResult(1)(values(0)(0))
    assertResult(4)(values(0)(1))
    assertResult(20.787)(values(0)(2))
    assert(TestUtils.equalsRounded(19.112667,values(0)(3),6))
    assertResult(5)(values(0)(4))
    assertResult(21.626)(values(0)(5))
    assert(TestUtils.equalsRounded(1.3413,values(0)(6),4))
    assert(TestUtils.equalsRounded(14.9195,values(0)(7),4))
  }
}
