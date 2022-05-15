package com.xdorg1.hudimomo.test

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class ClueTest extends FunSuite with SparkSessionTestWrapper with MockitoSugar
  with DataFrameComparer with Serializable {
  private val obj = new Clue(null, "20210806") {
    def save(spark: SparkSession, table: String, df: DataFrame, impDate: String): Unit = {}
  }
  test("testRemoveSymbolUdf") {
    import spark.implicits._

    val sourceDF = Seq(
      "1234这里this是is一个a测%试test案$例case     ",
      "这里是。.,;*&？*；&……一个1234测试案例"
    ).toDF("title")

    val actualDF = sourceDF.withColumn("title", obj.removeSymbolUdf(functions.col("title")))

    val expectedDF = Seq(
      "1234这里this是is一个a测试test案例case",
      "这里是一个1234测试案例"
    ).toDF("title")

    // assertSmallDataFrameEquality是包spark-fast-tests下DataFrameComparer的断言方法, 可用于比较DataFrame
    assertSmallDataFrameEquality(actualDF, expectedDF)
  }
}

