-- Flink SQL ops from hudi to mysql


-- 启动HDFS服务


-- 启动Flink Standalone集群

-- 启动SQL Client
/usr/local/src/flink/bin/sql-client.sh embedded -j /usr/local/src/flink/lib/hudi-flink-bundle_2.12-0.9.0.jar shell

-- 设置属性
set execution.result-mode=tableau;
set execution.checkpointing.interval=3sec;
-- 流处理模式
SET execution.runtime-mode = streaming;


-- =======================================================================================================
-- 1. 访问量：edu_web_chat_ems_hudi【Hudi】, realtime_web_pv【MySQL】
-- 2. 咨询量：edu_web_chat_ems_hudi【Hudi】, realtime_stu_consult【MySQL】
-- =======================================================================================================

-- 1) 统计当日访问量
-- =======================================================================================================
-- ============================================ 1. 当日访问量========================================
-- =======================================================================================================
-- 1-1. MySQL 数据库表
CREATE TABLE `itcast_rpt`.`realtime_web_pv` (
    `report_date` varchar(255) NOT NULL,
    `report_total` bigint(20) NOT NULL,
    PRIMARY KEY (`report_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- 1-2. Flink SQL Connector Hudi
CREATE TABLE edu_web_chat_ems_hudi (
   id string PRIMARY KEY NOT ENFORCED,
   create_date_time string,
   session_id string,
   sid string,
   create_time string,
   seo_source string,
   seo_keywords string,
   ip string,
   area string,
   country string,
   province string,
   city string,
   origin_channel string,
   `user` string,
   manual_time string,
   begin_time string,
   end_time string,
   last_customer_msg_time_stamp string,
   last_agent_msg_time_stamp string,
   reply_msg_count string,
   msg_count string,
   browser_name string,
   os_info string,
   part STRING
)
    PARTITIONED BY (part)
WITH(
  'connector'='hudi',
  'path'= 'hdfs://spark-master:9000/hudi-warehouse/edu_web_chat_ems_hudi',
  'table.type'= 'MERGE_ON_READ',
  'hoodie.datasource.write.recordkey.field'= 'id',
  'write.precombine.field'= 'create_date_time',
  'read.tasks' = '1'
);

-- 1-3. 指标计算
-- 统计结果，存储至视图View
CREATE VIEW IF NOT EXISTS view_tmp_web_pv AS
SELECT day_value, COUNT(id) AS total FROM (
      SELECT
          FROM_UNIXTIME(CAST(create_time AS BIGINT) / 1000, 'yyyy-MM-dd') AS day_value, id
      FROM edu_web_chat_ems_hudi
      WHERE part = '2022-05-17'
    ) GROUP BY  day_value;

--  1-4. 保存MySQL数据库
-- SQL Connector MySQL
CREATE TABLE realtime_web_pv_mysql (
   report_date STRING,
   report_total BIGINT,
   PRIMARY KEY (report_date) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://spark-master:3306/itcast_rpt',
   'driver' = 'com.mysql.cj.jdbc.Driver',
   'username' = 'hadoop',
   'password' = 'spark-master',
   'table-name' = 'realtime_web_pv'
);

-- INSERT INTO 插入
INSERT INTO  realtime_web_pv_mysql SELECT day_value, total FROM view_tmp_web_pv;



-- =======================================================================================================
-- ============================================ 2. 当日咨询量 ============================================
-- =======================================================================================================
-- 2-1. MySQL 数据库表
CREATE TABLE `itcast_rpt`.`realtime_stu_consult` (
     `report_date` varchar(255) NOT NULL,
     `report_total` bigint(20) NOT NULL,
     PRIMARY KEY (`report_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 2-2. Flink SQL Connector Hudi


-- 2-3. 指标计算
CREATE VIEW IF NOT EXISTS view_tmp_stu_consult AS
SELECT day_value, COUNT(id) AS total FROM (
  SELECT
      FROM_UNIXTIME(CAST(create_time AS BIGINT) / 1000, 'yyyy-MM-dd') AS day_value, id
  FROM edu_web_chat_ems_hudi
  WHERE part = '2022-05-17' AND msg_count > 0
) GROUP BY  day_value;

-- 2-4. 保存MySQL数据库
-- SQL Connector MySQL
CREATE TABLE realtime_stu_consult_mysql (
    report_date STRING,
    report_total BIGINT,
    PRIMARY KEY (report_date) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://spark-master:3306/itcast_rpt',
   'driver' = 'com.mysql.cj.jdbc.Driver',
   'username' = 'hadoop',
   'password' = 'spark-master',
   'table-name' = 'realtime_stu_consult'
);

-- INSERT INTO 插入
INSERT INTO  realtime_stu_consult_mysql SELECT day_value, total FROM view_tmp_stu_consult;



-- =======================================================================================================
-- 3. 意向数：edu_customer_relationship_hudi【Hudi】, realtime_stu_intention【MySQL】
-- 4. 报名人数：edu_customer_relationship_hudi【Hudi】, realtime_stu_apply【MySQL】
-- =======================================================================================================

-- =======================================================================================================
-- ============================================ 3. 今日意向数 ========================================
-- =======================================================================================================
-- 3. 今日意向数
-- 3-1. MySQL 数据库表
CREATE TABLE `itcast_rpt`.`realtime_stu_intention` (
   `report_date` varchar(255) NOT NULL,
   `report_total` bigint(20) NOT NULL,
   PRIMARY KEY (`report_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- 3-2. Flink SQL Connector Hudi
create table edu_customer_relationship_hudi(
   id string PRIMARY KEY NOT ENFORCED,
   create_date_time string,
   update_date_time string,
   deleted string,
   customer_id string,
   first_id string,
   belonger string,
   belonger_name string,
   initial_belonger string,
   distribution_handler string,
   business_scrm_department_id string,
   last_visit_time string,
   next_visit_time string,
   origin_type string,
   itcast_school_id string,
   itcast_subject_id string,
   intention_study_type string,
   anticipat_signup_date string,
   `level` string,
   creator string,
   current_creator string,
   creator_name string,
   origin_channel string,
   `comment` string,
   first_customer_clue_id string,
   last_customer_clue_id string,
   process_state string,
   process_time string,
   payment_state string,
   payment_time string,
   signup_state string,
   signup_time string,
   notice_state string,
   notice_time string,
   lock_state string,
   lock_time string,
   itcast_clazz_id string,
   itcast_clazz_time string,
   payment_url string,
   payment_url_time string,
   ems_student_id string,
   delete_reason string,
   deleter string,
   deleter_name string,
   delete_time string,
   course_id string,
   course_name string,
   delete_comment string,
   close_state string,
   close_time string,
   appeal_id string,
   tenant string,
   total_fee string,
   belonged string,
   belonged_time string,
   belonger_time string,
   transfer string,
   transfer_time string,
   follow_type string,
   transfer_bxg_oa_account string,
   transfer_bxg_belonger_name string,
   part STRING
)
    PARTITIONED BY (part)
WITH(
  'connector'='hudi',
  'path'= 'hdfs://spark-master:9000/hudi-warehouse/edu_customer_relationship_hudi',
  'table.type'= 'MERGE_ON_READ',
  'hoodie.datasource.write.recordkey.field'= 'id',
  'write.precombine.field'= 'create_date_time',
  'read.tasks' = '1'
);

-- 3-3. 指标计算
CREATE VIEW IF NOT EXISTS view_tmp_stu_intention AS
SELECT day_value, COUNT(id) AS total FROM (
  SELECT
      FROM_UNIXTIME(CAST(create_date_time AS BIGINT) / 1000, 'yyyy-MM-dd') AS day_value, id
  FROM edu_customer_relationship_hudi
  WHERE part = '2022-05-17' AND create_date_time IS NOT NULL AND deleted = 'false'
) GROUP BY  day_value;

-- 3-4. 保存MySQL数据库
-- SQL Connector MySQL
CREATE TABLE realtime_stu_intention_mysql (
  report_date STRING,
  report_total BIGINT,
  PRIMARY KEY (report_date) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://spark-master:3306/itcast_rpt',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'username' = 'hadoop',
    'password' = 'spark-master',
    'table-name' = 'realtime_stu_intention'
);

-- INSERT INTO 插入
INSERT INTO  realtime_stu_intention_mysql SELECT day_value, total FROM view_tmp_stu_intention;



-- =======================================================================================================
-- ============================================ 4. 今日报名人数 ============================================
-- =======================================================================================================
-- 4-1. MySQL 数据库表
CREATE TABLE `itcast_rpt`.`realtime_stu_apply` (
   `report_date` varchar(255) NOT NULL,
   `report_total` bigint(20) NOT NULL,
   PRIMARY KEY (`report_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 4-2. Flink SQL Connector Hudi


-- 4-3. 指标计算
CREATE VIEW IF NOT EXISTS view_tmp_stu_apply AS
SELECT day_value, COUNT(id) AS total FROM (
  SELECT
      FROM_UNIXTIME(CAST(payment_time AS BIGINT) / 1000, 'yyyy-MM-dd') AS day_value, id
  FROM edu_customer_relationship_hudi
  WHERE part = '2022-05-17' AND payment_time IS NOT NULL AND payment_state = 'PAID' AND deleted = 'false'
) GROUP BY  day_value;

-- 4-4. 保存MySQL数据库
-- SQL Connector MySQL
CREATE TABLE realtime_stu_apply_mysql (
  report_date STRING,
  report_total BIGINT,
  PRIMARY KEY (report_date) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://spark-master:3306/itcast_rpt',
   'driver' = 'com.mysql.cj.jdbc.Driver',
   'username' = 'hadoop',
   'password' = 'spark-master',
   'table-name' = 'realtime_stu_apply'
);

-- INSERT INTO 插入
INSERT INTO  realtime_stu_apply_mysql SELECT day_value, total FROM view_tmp_stu_apply;


-- =======================================================================================================
-- ============================================ 5. 今日有效线索量 ========================================
-- =======================================================================================================
-- 5-1. MySQL 数据库表
CREATE TABLE `itcast_rpt`.`realtime_stu_clue` (
  `report_date` varchar(255) NOT NULL,
  `report_total` bigint(20) NOT NULL,
  PRIMARY KEY (`report_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- 5-2. Flink SQL Connector Hudi
create table edu_customer_clue_hudi(
   id string PRIMARY KEY NOT ENFORCED,
   create_date_time string,
   update_date_time string,
   deleted string,
   customer_id string,
   customer_relationship_id string,
   session_id string,
   sid string,
   status string,
   `user` string,
   create_time string,
   platform string,
   s_name string,
   seo_source string,
   seo_keywords string,
   ip string,
   referrer string,
   from_url string,
   landing_page_url string,
   url_title string,
   to_peer string,
   manual_time string,
   begin_time string,
   reply_msg_count string,
   total_msg_count string,
   msg_count string,
   `comment` string,
   finish_reason string,
   finish_user string,
   end_time string,
   platform_description string,
   browser_name string,
   os_info string,
   area string,
   country string,
   province string,
   city string,
   creator string,
   name string,
   idcard string,
   phone string,
   itcast_school_id string,
   itcast_school string,
   itcast_subject_id string,
   itcast_subject string,
   wechat string,
   qq string,
   email string,
   gender string,
   `level` string,
   origin_type string,
   information_way string,
   working_years string,
   technical_directions string,
   customer_state string,
   valid string,
   anticipat_signup_date string,
   clue_state string,
   scrm_department_id string,
   superior_url string,
   superior_source string,
   landing_url string,
   landing_source string,
   info_url string,
   info_source string,
   origin_channel string,
   course_id string,
   course_name string,
   zhuge_session_id string,
   is_repeat string,
   tenant string,
   activity_id string,
   activity_name string,
   follow_type string,
   shunt_mode_id string,
   shunt_employee_group_id string,
   part STRING
)
    PARTITIONED BY (part)
WITH(
  'connector'='hudi',
  'path'= 'hdfs://spark-master:9000/hudi-warehouse/edu_customer_clue_hudi',
  'table.type'= 'MERGE_ON_READ',
  'hoodie.datasource.write.recordkey.field'= 'id',
  'write.precombine.field'= 'create_date_time',
  'read.tasks' = '1'
);

-- 5-3. 指标计算
SELECT id, create_date_time, deleted, clue_state FROM edu_customer_clue_hudi WHERE clue_state is not null LIMIT 10 ;

CREATE VIEW IF NOT EXISTS view_tmp_stu_clue AS
SELECT day_value, COUNT(id) AS total FROM (
  SELECT
      FROM_UNIXTIME(CAST(create_date_time AS BIGINT) / 1000, 'yyyy-MM-dd') AS day_value, id
  FROM edu_customer_clue_hudi
  WHERE part = '2022-05-17' AND clue_state IS NOT NULL AND deleted = 'false'
) GROUP BY  day_value;

-- 5-4. 保存MySQL数据库
-- SQL Connector MySQL
CREATE TABLE realtime_stu_clue_mysql (
     report_date STRING,
     report_total BIGINT,
     PRIMARY KEY (report_date) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://spark-master:3306/itcast_rpt',
   'driver' = 'com.mysql.cj.jdbc.Driver',
   'username' = 'hadoop',
   'password' = 'spark-master',
   'table-name' = 'realtime_stu_clue'
);

-- INSERT INTO 插入
INSERT INTO  realtime_stu_clue_mysql SELECT day_value, total FROM view_tmp_stu_clue;




