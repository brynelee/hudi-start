package com.xdorg1.hudimomo

import org.apache.spark.sql.functions.{concat, lit, substring, unix_timestamp}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * 编写StructuredStreaming流式程序：实时消费Kafka中Momo聊天数据，进行转换处理，保存至Hudi表
 */
object MomoStreamHudi {
  def main(args: Array[String]): Unit = {
    // step1、构建SparkSession实例对象
    val spark: SparkSession = createSparkSession(this.getClass)

    // step2、从Kafka实时消费数据
    val kafkaStreamDF: DataFrame = readFromKafka(spark, "7MO-MSG")

    // step3、提取数据，转换数据类型
    val streamDF: DataFrame = process(kafkaStreamDF)

    // step4、保存数据至Hudi表：MOR
    // printToConsole(streamDF)
    saveToHudi(streamDF)

    // step5、流式应用启动以后，等待终止
    spark.streams.active.foreach(query => println(s"Query: ${query.name} is Running ............."))
    spark.streams.awaitAnyTermination()
  }

  /**
   * 创建SparkSession会话实例对象，基本属性设置
   */
  def createSparkSession(clazz: Class[_]): SparkSession = {
    SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      // 设置序列化方式：Kryo
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置属性：Shuffle时分区数和并行度
      .config("spark.default.parallelism", 2)
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
      .getOrCreate()
  }

  /**
   * 指定Kafka Topic名称，实时消费数据
   */
  def readFromKafka(spark: SparkSession, topicName: String): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master:9092")
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", 100000)
      .option("failOnDataLoss", "false")
      .load()
  }

  /**
   * 对Kafka获取数据，进行转换操作，获取所有字段的值，转换为String，以便保存到Hudi表中
   */
  def process(streamDF: DataFrame): DataFrame = {
    import streamDF.sparkSession.implicits._

    // 1-提取Message消息数据
    val messageStreamDF: DataFrame = streamDF.selectExpr("CAST(value AS STRING) message")

    // 2-解析数据，封装实体类中
    val momoStreamDS: Dataset[MomoMessage] = messageStreamDF
      .as[String]
      .map(message => {
        val array = message.split("\001")
        val momoMessage = MomoMessage(
          array(0), array(1), array(2), array(3), array(4), array(5), array(6), array(7), array(8), array(9),
          array(10), array(11), array(12), array(13), array(14), array(15), array(16), array(17), array(18), array(19)
        )
        // 返回实体类
        momoMessage
      })

    // 3-向Hudi表写入数据时，考虑如下字段：主键id、数据聚合字段ts、分区字段day
    val hudiStreamDF: DataFrame = momoStreamDS
      .toDF()
      .withColumn("ts", unix_timestamp($"msg_time").cast(StringType))
      .withColumn(
        "message_id",
        concat($"sender_account", lit("_"), $"ts", lit("_"), $"receiver_account")
      )
      .withColumn("day", substring($"msg_time", 0, 10))

    // 返回DataFrame数据集
    hudiStreamDF
  }

  /**
   * 将流式数据集DataFrame保存至Hudi表，类型为MOR
   */
  def saveToHudi(streamDF: DataFrame): Unit = {

    streamDF.writeStream
      .outputMode(OutputMode.Append())
      .queryName("query-hudi-7mo")
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
        println(s"============== BatchId: $batchId start ==============")

        import org.apache.hudi.DataSourceWriteOptions._
        import org.apache.hudi.config.HoodieWriteConfig._
        import org.apache.hudi.keygen.constant.KeyGeneratorOptions._

        batchDF.write
          .format("hudi")
          .mode(SaveMode.Append)
          .option(TBL_NAME.key, "7mo_msg_hudi")
          .option(TABLE_TYPE.key(), "MERGE_ON_READ")
          .option(RECORDKEY_FIELD_NAME.key(), "message_id")
          .option(PRECOMBINE_FIELD_NAME.key(), "ts")
          .option(PARTITIONPATH_FIELD_NAME.key(), "day")
          .option(HIVE_STYLE_PARTITIONING_ENABLE.key(), "true")
          // 插入数据，产生shuffle时，分区数目
          .option("hoodie.insert.shuffle.parallelism", "2")
          .option("hoodie.upsert.shuffle.parallelism", "2")
          // 表数据存储路径
          .save("/hudi-warehouse/7mo_msg_hudi")
      })
      .option("checkpointLocation", "/datas/hudi-struct-ckpt")
      .start()
  }

  /**
   * 测试方法，将数据打印控制台
   */
  def printToConsole(streamDF: DataFrame): Unit = {
    streamDF.writeStream
      .outputMode(OutputMode.Append())
      .queryName("query-hudi-momo")
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .option("checkpointLocation", "/datas/hudi-struct-ckpt-0")
      .start()
  }
}
