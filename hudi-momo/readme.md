
# 7mo生成mock短信历史，进行数据分析



## 数据生成
使用已经开发好的工具生成数据文件

```shell script

java -jar /home/hadoop/hudi_demo/7mo_init/7Mo_DataGen.jar \
/home/hadoop/hudi_demo/7mo_init/7Mo_Data.xlsx \
/home/hadoop/hudi_demo/7mo_data \
5000

```

## 使用flume监测读取数据，投递到kafka

```properties

# define a1
a1.sources = s1 
a1.channels = c1
a1.sinks = k1

#define s1
a1.sources.s1.type = TAILDIR
#指定一个元数据记录文件
a1.sources.s1.positionFile = /usr/local/src/flume/position/taildir_7mo_kafka.json
#将所有需要监控的数据源变成一个组
a1.sources.s1.filegroups = f1
#指定了f1是谁：监控目录下所有文件
a1.sources.s1.filegroups.f1 = /home/hadoop/hudi_demo/7mo_data/.*
#指定f1采集到的数据的header中包含一个KV对
a1.sources.s1.headers.f1.type = 7mo
a1.sources.s1.fileHeader = true

#define c1
a1.channels.c1.type = memory
a1.channels.c1.capacity = 10000
a1.channels.c1.transactionCapacity = 1000

#define k1
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.kafka.topic = 7MO-MSG
a1.sinks.k1.kafka.bootstrap.servers = spark-master:9092
a1.sinks.k1.kafka.flumeBatchSize = 10
a1.sinks.k1.kafka.producer.acks = 1
a1.sinks.k1.kafka.producer.linger.ms = 100

#bind
a1.sources.s1.channels = c1
a1.sinks.k1.channel = c1

```

## 编写StructuredStreaming流式程序：实时消费Kafka中Momo聊天数据，进行转换处理，保存至Hudi表

```scala

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


```

## 编写SparkSQL程序，基于DSL和SQL分析Hudi表数据，最终存储至MySQL数据库表中

```scala

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


```
