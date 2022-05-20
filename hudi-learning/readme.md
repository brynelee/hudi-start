
# hudi table read and write using spark APIs in Scala

## scala/HudiSparkDemo

操作结构整体如下：

```scala

  def main(args: Array[String]): Unit = {


    // 创建SparkSession实例对象，设置属性
    val spark: SparkSession = {
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }

    // 定义变量、表名称，保存路径
    val tableName:String = "tbl_trip_cow"
    val tablePath:String = "/hudi-warehouse/tbl_trips_cow"

    // 构建数据生成器，模拟产生业务数据
    import org.apache.hudi.QuickstartUtils._

    // 任务一：模拟数据，插入hudi表，采用COW模式
    insertData(spark, tableName, tablePath)

    // 任务二：快照方式查询（Snapshot Query）数据，采用DSL方式
    queryData(spark, tablePath)

    // 任务三：更新（Update）数据，第1步、模拟产生数据，第2步、模拟产生数据，针对第1步数据字段值更新，第3步、将数据更新到Hudi表中
    val dataGen: DataGenerator = new DataGenerator()
    insertData(spark, tableName, tablePath, dataGen)
    updateData(spark, tableName, tablePath, dataGen)

    // 任务四：增量查询（Incremental Query）数据，采用SQL方式
     incrementalQueryData(spark, tablePath)

    // 任务五：删除（Delete）数据
     deleteData(spark, tableName, tablePath)

    spark.stop()
  }

```