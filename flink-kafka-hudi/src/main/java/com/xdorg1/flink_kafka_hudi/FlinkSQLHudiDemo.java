package com.xdorg1.flink_kafka_hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 基于Flink SQL Connector实现：实时消费Topic中数据，转换处理后，实时存储到Hudi表中
 */
public class FlinkSQLHudiDemo {

    public static void main(String[] args) {
        // 1-获取表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO： 由于增量将数据写入到Hudi表，所以需要启动Flink Checkpoint检查点
        env.setParallelism(1);
        env.enableCheckpointing(5000) ;
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 设置流式模式
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2-创建输入表，TODO：从Kafka消费数据
        tableEnv.executeSql(
                "CREATE TABLE order_kafka_source (\n" +
                        "  orderId STRING,\n" +
                        "  userId STRING,\n" +
                        "  orderTime STRING,\n" +
                        "  ip STRING,\n" +
                        "  orderMoney DOUBLE,\n" +
                        "  orderStatus INT\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'order-topic',\n" +
                        "  'properties.bootstrap.servers' = 'spark-master:9092',\n" +
                        "  'properties.group.id' = 'gid-1002',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.fail-on-missing-field' = 'false',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );

        // 3-转换数据：可以使用SQL，也可以时Table API
        Table etlTable = tableEnv
                .from("order_kafka_source")
                // 添加字段：Hudi表数据合并字段，时间戳, "orderId": "20211122103434136000001" ->  20211122103434136
                .addColumns(
                        $("orderId").substring(0, 17).as("ts")
                )
                // 添加字段：Hudi表分区字段， "orderTime": "2021-11-22 10:34:34.136" -> 021-11-22
                .addColumns(
                        $("orderTime").substring(0, 10).as("partition_day")
                );
        tableEnv.createTemporaryView("view_order", etlTable);

        // 4-创建输出表，TODO: 关联到Hudi表，指定Hudi表名称，存储路径，字段名称等等信息
        tableEnv.executeSql(
                "CREATE TABLE order_hudi_sink (\n" +
                        "  orderId STRING PRIMARY KEY NOT ENFORCED,\n" +
                        "  userId STRING,\n" +
                        "  orderTime STRING,\n" +
                        "  ip STRING,\n" +
                        "  orderMoney DOUBLE,\n" +
                        "  orderStatus INT,\n" +
                        "  ts STRING,\n" +
                        "  partition_day STRING\n" +
                        ")\n" +
                        "PARTITIONED BY (partition_day)\n" +
                        "WITH (\n" +
                        "    'connector' = 'hudi',\n" +
                        "    'path' = 'file:///D:/tmp/flink_hudi_order',\n" +
                        "    'table.type' = 'MERGE_ON_READ',\n" +
                        "    'write.operation' = 'upsert',\n" +
                        "    'hoodie.datasource.write.recordkey.field'= 'orderId',\n" +
                        "    'write.precombine.field' = 'ts',\n" +
                        "    'write.tasks'= '1'\n" +
                        ")"
        );

        // 5-通过子查询方式，将数据写入输出表
        tableEnv.executeSql(
                "INSERT INTO order_hudi_sink " +
                        "SELECT orderId, userId, orderTime, ip, orderMoney, orderStatus, ts, partition_day FROM view_order"
        );

    }

}
