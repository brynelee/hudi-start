package com.xdorg1.hudimomo.test

import org.apache.hadoop.hive.ql.exec.UDF

class ScalaUDFTest extends UDF {
  // 测试，为输入拼接前缀
  def evaluate(input: String): String = {
    return "UDF Scala: " + input
  }
}

object ScalaUDFTest{
  def main(args: Array[String]) {
    var obj = new ScalaUDFTest();
    println(obj.evaluate("xxx"));
  }
}

