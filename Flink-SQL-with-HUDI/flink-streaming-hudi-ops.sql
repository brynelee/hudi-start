## Flink stream SQL 操作（基于hudi表）

CREATE TABLE t2(
  uuid VARCHAR(20),
  name VARCHAR(10),
  age INT,
  ts TIMESTAMP(3),
  `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://spark-master:9000/hudi-warehouse/hudi-t1',
  'table.type' = 'MERGE_ON_READ',
  'read.tasks' = '1',
  'read.streaming.enabled' = 'true',
  'read.streaming.start-commit' = '20210316134557',
  'read.streaming.check-interval' = '4'
);


select * from t2;

## 启动第3个sql-client.sh

/usr/local/src/flink/bin/sql-client.sh embedded shell

set execution.result-mode=tableau;

## 重新创建t1表，与前面的t1定义完全一致

CREATE TABLE t1(
   uuid VARCHAR(20),
   name VARCHAR(10),
   age INT,
   ts TIMESTAMP(3),
   `partition` VARCHAR(20)
)
PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://spark-master:9000/hudi-warehouse/hudi-t1',
  'write.tasks' = '1',
  'compaction.tasks' = '1',
  'table.type' = 'MERGE_ON_READ'
);

INSERT INTO t1 VALUES
  ('id9','Brian',48,TIMESTAMP '1974-01-20 00:00:07','par3'),
  ('id10','Tom',65,TIMESTAMP '1972-01-15 00:00:08','par2');