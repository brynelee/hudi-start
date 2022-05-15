package com.xdorg1.hudimomo.test

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test example")
      .config("spark.driver.bindAddress","127.0.0.1")
      .getOrCreate()
  }
}
