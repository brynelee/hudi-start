package com.xdorg1.hudimomo

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * 编写SparkSQL程序，基于DSL和SQL分析Hudi表数据，最终存储至MySQL数据库表中
 */
object MomoSQLHudi {

  def main(args: Array[String]): Unit = {
    // step1、构建SparkSession实例对象
    val spark: SparkSession = createSparkSession(this.getClass)

    // step2、加载Hudi表数据，指定表数据存储路径
    val hudiDF: DataFrame = loadHudiTable(spark, "/hudi-warehouse/7mo_msg_hudi")
    //		println(s"Count = ${hudiDF.count()}")
    //		hudiDF.printSchema()
    //		hudiDF.show(numRows = 10, truncate = false)

    // step3、数据进行ETL转换：提取字段，解析IP地址为省份，使用ip2Region库解析
    val etlDF: DataFrame = etl(hudiDF)
    		etlDF.printSchema()
    		etlDF.show(numRows = 10, truncate = false)

    // step4、业务指标分析
    process(etlDF)

    // step5、应用结束，关闭资源
    spark.stop()
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
      .getOrCreate()
  }

  /**
   * 指定Hudi表数据存储路径path，加载Hudi表数据，返回DataFrame
   */
  def loadHudiTable(spark: SparkSession, tablePath: String): DataFrame = {
    spark.read.format("hudi").load(tablePath)
  }

  /**
   * 提取字段数据和转换ip地址为省份
   */
  def etl(dataframe: DataFrame): DataFrame = {

    val session: SparkSession = dataframe.sparkSession

    // 1-自定义UDF函数，解析IP地址为省份
    session.udf.register(
      "ip_to_province",
      (ip: String) => {
        // a. 创建DbSearcher对象
        val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db")
        // b. 解析IP地址
        val dataBlock: DataBlock = dbSearcher.btreeSearch(ip)
        // c. 获取解析数据
        val region: String = dataBlock.getRegion
        // d. 获取省份
        val Array(_, _, province, _, _ ) = region.split("\\|")
        // 返回省份
        province
      }
    )

    // 2-提取字段和解析IP地址
    dataframe.createOrReplaceTempView("view_tmp_momo")
    val etlDF: DataFrame = session.sql(
      """
			  |SELECT
			  |  day, message_id, sender_nickyname, receiver_nickyname,
			  |  ip_to_province(sender_ip) AS sender_province,
			  |  ip_to_province(receiver_ip) AS receiver_province
			  |FROM
			  |  view_tmp_momo
			  |""".stripMargin
    )

    // 返回数据集
    etlDF
  }

  /**
   * 按照业务指标分析数据
   */
  def process(dataframe: DataFrame): Unit = {
    val session: SparkSession = dataframe.sparkSession

    // 1-将DataFrame注册为临时视图
    dataframe.createOrReplaceTempView("view_tmp_etl")
    // 优化：由于数据集被使用多次，将其进行持久化缓存
    session.catalog.cacheTable("view_tmp_etl", storageLevel = StorageLevel.MEMORY_AND_DISK)

    // 2-指标1：统计总信息量
    val reportAllTotalDF: DataFrame = session.sql(
      """
			  |WITH tmp AS(
			  |  SELECT COUNT(1) AS 7mo_total FROM view_tmp_etl
			  |)
			  |SELECT "全国" AS 7mo_name, 7mo_total, "1" AS 7mo_category FROM tmp
			  |""".stripMargin
    )
    //		reportAllTotalDF.printSchema()
    //		reportAllTotalDF.show()

    // 2-指标2：统计各个省份发送信息量
    val reportSenderProvinceTotalDF: DataFrame = session.sql(
      """
			  |WITH tmp AS(
			  |  SELECT sender_province, COUNT(1) AS 7mo_total FROM view_tmp_etl GROUP BY sender_province
			  |)
			  |SELECT sender_province AS 7mo_name, 7mo_total, "2" AS 7mo_category FROM tmp
			  |""".stripMargin
    )

    // 2-指标3：统计各个省份接收信息量
    val reportReceiverProvinceTotalDF: DataFrame = session.sql(
      """
			  |WITH tmp AS(
			  |  SELECT receiver_province, COUNT(1) AS 7mo_total FROM view_tmp_etl GROUP BY receiver_province
			  |)
			  |SELECT receiver_province AS 7mo_name, 7mo_total, "3" AS 7mo_category FROM tmp
			  |""".stripMargin
    )

    // 2-指标4：统计各个用户，发送信息量
    val reportSenderNickynameTotalDF: DataFrame = session.sql(
      """
			  |WITH tmp AS(
			  |  SELECT sender_nickyname, COUNT(1) AS 7mo_total FROM view_tmp_etl GROUP BY sender_nickyname
			  |)
			  |SELECT sender_nickyname AS 7mo_name, 7mo_total, "4" AS 7mo_category FROM tmp
			  |""".stripMargin
    )

    // 2-指标5：统计各个用户，发送信息量
    val reportReceiverNickynameTotalDF: DataFrame = session.sql(
      """
			  |WITH tmp AS(
			  |  SELECT receiver_nickyname, COUNT(1) AS 7mo_total FROM view_tmp_etl GROUP BY receiver_nickyname
			  |)
			  |SELECT receiver_nickyname AS 7mo_name, 7mo_total, "5" AS 7mo_category FROM tmp
			  |""".stripMargin
    )

    // TODO: 将上述统计5个指标结果，合并为一个DataFrame
    val reportTotalDF: DataFrame = reportAllTotalDF
      .union(reportSenderProvinceTotalDF)
      .union(reportReceiverProvinceTotalDF)
      .union(reportSenderNickynameTotalDF)
      .union(reportReceiverNickynameTotalDF)
    // reportTotalDF.show(500, truncate = false)

    // 当缓存数据不再使用时，释放资源
    session.catalog.uncacheTable("view_tmp_etl")

    // 3-保存指标结果数据到MySQL数据库表
    saveToMysql(reportTotalDF)
  }

  /**
   * 将DataFrame数据集，使用SparkSQL中外部数据源接口，保存数据到MySQL表中
   */
  def saveToMysql(dataframe: DataFrame): Unit = {
    dataframe
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", "jdbc:mysql://spark-master:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "7mo.7mo_report")
      .option("user", "hadoop")
      .option("password", "spark-master")
      .option("batchsize", "1000")
      .save()
  }
}
