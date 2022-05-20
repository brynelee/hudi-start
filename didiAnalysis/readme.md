
# Didi case Analysis

## hive-analysis

使用hive HQL在beeline环境下，进行数据分析。

## src

使用scala语言

### didi/DidiStorageSpark
使用spark API读取数据文件，加工处理ETL，存入hudi

 * 滴滴海口出行运营数据分析，使用SparkSQL操作数据，先读取CSv文件，保存至Hudi表中。
 * step1. 构建SparkSession实例对象（集成Hudi和HDFS）
 * step2. 加载本地CSV文件格式滴滴出行数据
 * step3. 滴滴出行数据ETL处理
 * stpe4. 保存转换后数据至Hudi表
 * step5. 应用结束关闭资源
 
### didi/DidiAnalysisSpark

滴滴海口出行运营数据分析，使用SparkSQL操作数据，加载Hudi表数据，按照业务需求统计。

使用UDF进行数据分析

### stream/MockOrderProducer

自动生成mock订单数据，发送到kafka

### stream/HudiStructuredDemo

从kafka读取数据，ETL之后存储到hudi