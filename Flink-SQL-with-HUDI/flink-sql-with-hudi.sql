# 使用Flink sql-client.sh 来借助hudi connector来操作hudi表，进行SQL数据操作。
# 数据存储在HDFS

# 在SQL Client命令行提示符环境，设置结果展示方式

set execution.result-mode=tableau;

# 创建hudi表，放到HDFS

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
('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1');


INSERT INTO t1 VALUES
  ('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),
  ('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),
  ('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),
  ('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),
  ('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),
  ('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),
  ('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),
  ('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4');

# 按分区查询
select * from t1 where `partition` = 'par1';

# upsert操作

insert into t1 values ('id1','Danny',27,TIMESTAMP '1970-01-01 00:00:01','par1');