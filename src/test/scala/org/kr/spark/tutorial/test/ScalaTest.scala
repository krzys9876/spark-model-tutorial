package org.kr.spark.tutorial.test

import org.kr.spark.tutorial.{Base, BaseWithInput}
import org.scalatest.GivenWhenThen
import org.scalatest.funsuite.AnyFunSuite

class ScalaTest extends AnyFunSuite with GivenWhenThen {
  test("linear reggression calculation test for a single period") {
    Given("base from previous period with input data")
    val joinedBaseWithInput=
      BaseWithInput(1,Some(3L),Some(17.81),Some(20.592),Some(4L),Some(19.1126666666667),Some(0.838999999999989),
        Some(15.7566666666667),Some(3L),Some(106.286),Some(6L),Some(52.304),Some(14L),4,20.787)
    When("current base is calculated")
    val newBase=Base(joinedBaseWithInput)
    Then("values are equal to reference data")
    val reference=new Base(1,4,20.787,Some(19.112667),5,Some(21.626),
      Some(1.3413),Some(14.9195),4,189.434,10,73.091,30)
    assert(TestUtils.baseEquals(reference,newBase,6))
  }

  test("linear reggression calculation test for the first period") {
    Given("base from previous period with input data")
    val joinedBaseWithInput=
      BaseWithInput(1,None,None,None,None,None,None,None,None,None,None,None,None,1,16.132)
    When("current base is calculated")
    val newBase=Base(joinedBaseWithInput)
    Then("values are equal to reference data and linear reggresion is not calculated")
    val reference=new Base(1,1,16.132,None,2,None,None,None,
      1,16.132,1,16.132,1)
    assert(TestUtils.baseEquals(reference,newBase,6))
  }

  test("linear reggression calculation test for the second period") {
    Given("base from previous period with input data")
    val joinedBaseWithInput=
      BaseWithInput(1,Some(1),Some(16.132),None,Some(2),None,None,None,Some(1),Some(16.132),
        Some(1),Some(16.132),Some(1),2,18.362)
    When("current base is calculated")
    val newBase=Base(joinedBaseWithInput)
    Then("values are equal to reference data and linear reggresion is calculated")
    val reference=new Base(1,2,18.362,None,3,Some(20.592),Some(2.23),Some(13.902),
      2,52.856,3,34.494,5)
    assert(TestUtils.baseEquals(reference,newBase,6))
  }
}
