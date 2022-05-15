package com.xdorg1.hudimomo.test

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.UserDefinedFunction

class Clue(spark: SparkSession, ds: String) extends Serializable {
  // 去除标点符号和空格, 仅保留中文英文数字
  val removeSymbolUdf: UserDefinedFunction = functions.udf((title: String) => {
    val pattern = "[\\u4e00-\\u9fa5a-zA-Z0-9]+".r
    val matchess = pattern.findAllMatchIn(title)
    val buffer = new StringBuilder()
    matchess.foreach(matches => buffer.append(matches.group(0)))
    buffer.toString()
  })
}

