
# itcast_nev教育案例分析


## 数据准备

通过已经准备好的.sql文件的脚本导入MySQL数据库

## 使用Flink sql-client以及mysql connector将数据从MySQL读取，通过 hudi connector将数据写入hudi

参考：flink-SQL-hudi-to-mysql.sql

## 通过hive beeline在hive创建对应hudi数据表的外部表，为presto的使用做准备

参考:hudi-edu-hive.sql

## 通过presto对hive数据进行分析，并将结果写入MySQL

参考：hudi-to-mysql-by-presto.sql

## Flink CDC Java 代码结构样例

```java

package cn.itcast.hudi.edu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 基于Flink CDC Connector实现：实时获取MySQL数据库中传智教育业务数据，转换处理后，实时存储Hudi表
 */
public class EduDataStreamHudi {

	public static void main(String[] args) {
		// 1-获取表执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		env.enableCheckpointing(5000);
		EnvironmentSettings settings = EnvironmentSettings
			.newInstance()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings) ;

		// 2-基于CDC方式，将数据库MySQL中表数据实时增量同步到Hudi表

	}

	/**
	 * 具体实现CDC 操作，基于Flink SQL完成
	 */
	public static void cdcMySQLToHudi(StreamTableEnvironment tableEnv){
		// step1、创建输入表 - TODO: CDC
		tableEnv.executeSql("CREATE TABLE ....");

		// step2、创建输出表
		tableEnv.executeSql("CREATE TABLE ....") ;

		// step3、查询输入表数据，并且可以进行处理操作，最后插入到输出表中
		tableEnv.executeSql("INSERT INTO ... SELECT ...") ;
	}

}

```