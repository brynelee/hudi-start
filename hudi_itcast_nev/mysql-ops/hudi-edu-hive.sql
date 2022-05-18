-- 基于hudi数据来创建hive的外部表，映射到hudi的数据表

show databases ;

-- 创建数据库
CREATE DATABASE IF NOT EXISTS edu_hudi ;
-- 使用数据库
USE edu_hudi ;

-- =======================================================================================================
-- ======================================== 1.客户信息表【customer】 =====================================
-- =======================================================================================================
-- 1. 客户信息表【customer】
CREATE EXTERNAL TABLE edu_hudi.tbl_customer(
   id string,
   customer_relationship_id string,
   create_date_time string,
   update_date_time string,
   deleted string,
   name string,
   idcard string,
   birth_year string,
   gender string,
   phone string,
   wechat string,
   qq string,
   email string,
   area string,
   leave_school_date string,
   graduation_date string,
   bxg_student_id string,
   creator string,
   origin_type string,
   origin_channel string,
   tenant string,
   md_id string
)PARTITIONED BY (day_str string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    '/hudi-warehouse/edu_customer_hudi' ;

ALTER TABLE edu_hudi.tbl_customer ADD IF NOT EXISTS PARTITION(day_str='2022-05-16')
location '/hudi-warehouse/edu_customer_hudi/2022-05-16' ;

SELECT COUNT(1) AS total FROM edu_hudi.tbl_customer WHERE day_str = '2022-05-16' ;

SELECT id, name, gender, create_date_time FROM edu_hudi.tbl_customer WHERE day_str = '2022-05-16' LIMIT 10 ;

-- =======================================================================================================
-- ================================ 2.客户意向表【customer_relationship】=================================
-- =======================================================================================================
-- 2. 客户意向表【customer_relationship】
CREATE EXTERNAL TABLE edu_hudi.tbl_customer_relationship(
    id string,
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
    transfer_bxg_belonger_name string
)PARTITIONED BY (day_str string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'/hudi-warehouse/edu_customer_relationship_hudi' ;

ALTER TABLE edu_hudi.tbl_customer_relationship ADD IF NOT EXISTS PARTITION(day_str='2022-05-17')
location '/hudi-warehouse/edu_customer_relationship_hudi/2022-05-17' ;

SELECT COUNT(1) AS total FROM edu_hudi.tbl_customer_relationship WHERE day_str = '2022-05-17' ;

SELECT id, course_name, origin_type, create_date_time
FROM edu_hudi.tbl_customer_relationship WHERE day_str = '2022-05-17' LIMIT 10;



-- =======================================================================================================
-- ==================================== 3.客户线索表【customer_clue】 ====================================
-- =======================================================================================================
-- 3. 客户线索表【customer_clue】
CREATE EXTERNAL TABLE edu_hudi.tbl_customer_clue(
    id string,
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
    shunt_employee_group_id string
)
PARTITIONED BY (day_str string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'/hudi-warehouse/edu_customer_clue_hudi' ;

ALTER TABLE edu_hudi.tbl_customer_clue ADD IF NOT EXISTS PARTITION(day_str='2022-05-17')
location '/hudi-warehouse/edu_customer_clue_hudi/2022-05-17' ;

SELECT COUNT(1) AS total FROM edu_hudi.tbl_customer_clue WHERE day_str = '2022-05-17' ;

SELECT id, customer_id, s_name, create_date_time
FROM edu_hudi.tbl_customer_clue WHERE day_str = '2022-05-17' LIMIT 10 ;



-- =======================================================================================================
-- ================================ 4.客户申诉表【customer_appeal】 ======================================
-- =======================================================================================================
-- 4.客户申诉表【customer_appeal】
CREATE EXTERNAL TABLE edu_hudi.tbl_customer_appeal(
  id string,
  customer_relationship_first_id STRING,
  employee_id STRING,
  employee_name STRING,
  employee_department_id STRING,
  employee_tdepart_id STRING,
  appeal_status STRING,
  audit_id STRING,
  audit_name STRING,
  audit_department_id STRING,
  audit_department_name STRING,
  audit_date_time STRING,
  create_date_time STRING,
  update_date_time STRING,
  deleted STRING,
  tenant STRING
)
PARTITIONED BY (day_str string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'/hudi-warehouse/edu_customer_appeal_hudi' ;

ALTER TABLE edu_hudi.tbl_customer_appeal ADD IF NOT EXISTS PARTITION(day_str='2022-05-17')
location '/hudi-warehouse/edu_customer_appeal_hudi/2022-05-17' ;

SELECT COUNT(1) AS total FROM edu_hudi.tbl_customer_appeal WHERE day_str = '2022-05-17';

SELECT id, employee_id, employee_name, create_date_time
FROM tbl_customer_appeal WHERE day_str = '2022-05-17' LIMIT 10;

-- =======================================================================================================
-- ================================ 5.客服访问咨询记录表【web_chat_ems】 =================================
-- =======================================================================================================
-- 5. 客服访问咨询记录表【web_chat_ems】
CREATE EXTERNAL TABLE edu_hudi.tbl_web_chat_ems (
    id string,
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
    os_info string
)
PARTITIONED BY (day_str string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'/hudi-warehouse/edu_web_chat_ems_hudi' ;

ALTER TABLE edu_hudi.tbl_web_chat_ems ADD IF NOT EXISTS PARTITION(day_str='2022-05-17')
location '/hudi-warehouse/edu_web_chat_ems_hudi/2022-05-17' ;

SELECT COUNT(1) AS total FROM edu_hudi.tbl_web_chat_ems WHERE day_str = '2022-05-17';
SELECT id, session_id, ip, province FROM edu_hudi.tbl_web_chat_ems WHERE day_str = '2022-05-17' LIMIT 10;