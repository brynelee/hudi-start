
drop table db_hudi.tbl_7mo_hudi;

-- 创建表，关联Hudi表中
CREATE EXTERNAL TABLE db_hudi.tbl_7mo_hudi(
      msg_time             String,
      sender_nickyname     String,
      sender_account       String,
      sender_sex           String,
      sender_ip            String,
      sender_os            String,
      sender_phone_type    String,
      sender_network       String,
      sender_gps           String,
      receiver_nickyname   String,
      receiver_ip          String,
      receiver_account     String,
      receiver_os          String,
      receiver_phone_type  String,
      receiver_network     String,
      receiver_gps         String,
      receiver_sex         String,
      msg_type             String,
      distance             String,
      message              String,
      message_id           String,
      ts                   String
)
PARTITIONED BY (day string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hudi.hadoop.HoodieParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/hudi-warehouse/7mo_msg_hudi' ;


-- 添加分区信息
ALTER TABLE db_hudi.tbl_7mo_hudi
ADD IF NOT EXISTS PARTITION (day = '2022-05-15')
LOCATION '/hudi-warehouse/7mo_msg_hudi/day=2022-05-15' ;

-- 显示表的分区信息
SHOW PARTITIONS db_hudi.tbl_7mo_hudi ;

show tables;

-- 查询数据，前10条即可
SELECT
    msg_time, sender_nickyname, receiver_nickyname, ts, message
FROM db_hudi.tbl_7mo_hudi
WHERE day = '2022-05-15'
LIMIT 10 ;

-- 设置本地模式运行，以及非严格模式
-- set hive.exec.mode.local.auto=true;
set hive.mapred.mode=nonstrict;
-- 因为采用的是集群模式，不能使用本地模式执行
set hive.exec.mode.local.auto=false;

-- 指标1：统计总信息量
SELECT COUNT(1) AS momo_total FROM db_hudi.tbl_7mo_hudi WHERE day = '2022-05-15' ;

WITH tmp AS (
    SELECT COUNT(1) AS momo_total FROM db_hudi.tbl_7mo_hudi WHERE day = '2022-05-15'
)
SELECT "全国" AS momo_name, momo_total FROM  tmp ;

-- 指标2： 统计每个用户，发送信息量
WITH tmp AS (
    SELECT sender_nickyname, COUNT(1) AS momo_total FROM db_hudi.tbl_7mo_hudi
    WHERE day = '2022-05-15' GROUP BY sender_nickyname
)
SELECT sender_nickyname AS momo_name, momo_total FROM tmp ORDER BY momo_total DESC  LIMIT  10 ;


-- 指标3： 统计每个用户，接收信息量
WITH tmp AS (
    SELECT receiver_nickyname, COUNT(1) AS momo_total FROM db_hudi.tbl_7mo_hudi
    WHERE day = '2022-05-15' GROUP BY receiver_nickyname
)
SELECT receiver_nickyname AS momo_name, momo_total FROM tmp ORDER BY momo_total DESC  LIMIT  10 ;


-- 指标4：统计男女发送信息量
SELECT sender_sex, receiver_sex, COUNT(1) AS momo_total FROM db_hudi.tbl_7mo_hudi
WHERE day = '2022-05-15' GROUP BY sender_sex, receiver_sex ;

