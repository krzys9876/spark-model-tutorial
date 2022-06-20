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
    val sensor1= df
        .filter(col("sensor").equalTo(1))
        .collect()(0)
    val sensor2= df
      .filter(col("sensor").equalTo(2))
      .collect()(0)
    //copy from input file
    val sensor1ref=Array(1,4,20.787)
    val sensor2ref=Array(2,4,11.053)

    assert(TestUtils.equalsRounded(sensor1ref,sensor1,6),"input data for sensor 1")
    assert(TestUtils.equalsRounded(sensor2ref,sensor2,6),"input data for sensor 2")
  }

  test("Base file should be properly read to data frame") {
    Given("base file and spark session exist")
    assert(Files.exists(Path.of("base_test01.csv")))
    When("base file is read by spark session to data frame")
    val df=FileLoader.loadCSV("base_test01.csv",Schemas.base)
    Then("data frame contains data from file")
    val sensor1= df
      .filter(col("sensor").equalTo(1))
      .collect()
    val sensor2= df
      .filter(col("sensor").equalTo(2))
      .collect()
    assertResult(16.132)(sensor1(0)(2))
    assertResult(20.592)(sensor1(1)(5))
    assert(TestUtils.equalsRounded(15.756667,sensor1(2)(7),6))
    assertResult(2)(sensor2(0)(0))
    assert(TestUtils.equalsRounded(0.039,sensor2(1)(6),6))
    assert(TestUtils.equalsRounded(31.842,sensor2(2)(11),6))
  }

  test("Calculated base for next period should equal to reference data (spark functions model)") {
    Given("base file exist")
    assert(Files.exists(Path.of("base_test01.csv")))
    And("input file exists")
    assert(Files.exists(Path.of("input_test01.csv")))
    When("current period data is processed")
    val df=SparkModel.processSparkFunctions("base_test01.csv","input_test01.csv")
    Then("data frame contains data from file")
    val sensor1=TestUtils.getOutputRow(df,1,4)
    val sensor2=TestUtils.getOutputRow(df,2,4)
    //copy from reference file
    val sensor1ref=Array(1,4,20.787,19.112667,5,21.626,1.3413,14.9195,4,189.434,10,73.091,30)
    val sensor2ref=Array(2,4,11.053,11.442,5,11.467,0.2973,9.9805,4,108.724,10,42.895,30)

    assert(TestUtils.equalsRounded(sensor1ref,sensor1,6),"reference data for sensor 1")
    assert(TestUtils.equalsRounded(sensor2ref,sensor2,6),"reference data for sensor 2")
  }

  test("Calculated base for next period should equal to reference data (spark SQL model)") {
    Given("base file exist")
    assert(Files.exists(Path.of("base_test01.csv")))
    And("input file exists")
    assert(Files.exists(Path.of("input_test01.csv")))
    When("current period data is processed")
    val df=SparkModel.processSparkSQL("base_test01.csv","input_test01.csv")
    Then("data frame contains data from file")
    val sensor1=TestUtils.getOutputRow(df,1,4)
    val sensor2=TestUtils.getOutputRow(df,2,4)
    //copy from reference file
    val sensor1ref=Array(1,4,20.787,19.112667,5,21.626,1.3413,14.9195,4,189.434,10,73.091,30)
    val sensor2ref=Array(2,4,11.053,11.442,5,11.467,0.2973,9.9805,4,108.724,10,42.895,30)

    assert(TestUtils.equalsRounded(sensor1ref,sensor1,6),"reference data for sensor 1")
    assert(TestUtils.equalsRounded(sensor2ref,sensor2,6),"reference data for sensor 2")
  }

  test("Calculated base for next period should equal to reference data (spark with scala case classes)") {
    Given("base file exist")
    assert(Files.exists(Path.of("base_test01.csv")))
    And("input file exists")
    assert(Files.exists(Path.of("input_test01.csv")))
    When("current period data is processed")
    val df=SparkModel.processSparkScala("base_test01.csv","input_test01.csv")
    Then("data frame contains data from file")
    val sensor1=TestUtils.getOutputRow(df,1,4)
    val sensor2=TestUtils.getOutputRow(df,2,4)
    //copy from reference file
    val sensor1ref=Array(1,4,20.787,19.112667,5,21.626,1.3413,14.9195,4,189.434,10,73.091,30)
    val sensor2ref=Array(2,4,11.053,11.442,5,11.467,0.2973,9.9805,4,108.724,10,42.895,30)

    assert(TestUtils.equalsRounded(sensor1ref,sensor1,6),"reference data for sensor 1")
    assert(TestUtils.equalsRounded(sensor2ref,sensor2,6),"reference data for sensor 2")
  }
}
