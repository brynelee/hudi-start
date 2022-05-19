
show databases ;

use itcast_rpt;

-- itcast_rpt.stu_apply definition
CREATE TABLE  IF NOT EXISTS `itcast_rpt`.`stu_apply` (
     `report_date` longtext,
     `report_total` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

show tables ;

-- 1. 每日报名量
-- 1-1. 日期函数使用
SELECT from_unixtime(1330732800) AS date_str ;
SELECT from_unixtime(1330732800000/1000) AS date_str ;
SELECT format_datetime(from_unixtime(1330732800), 'yyyy-MM-dd') AS date_str ;


-- 1-2. 日期时间Long类型转换和格式化，应用业务
/*
字段：payment_time，支付状态变动时间
	select payment_time from hive.edu_hudi.tbl_customer_relationship WHERE payment_time is not null limit 10 ;

*/
SELECT
    format_datetime(from_unixtime(cast(payment_time as bigint) / 1000),'yyyy-MM-dd')AS day_value, customer_id
FROM
    hive.edu_hudi.tbl_customer_relationship
WHERE
        day_str = '2022-05-17' AND payment_time IS NOT NULL
LIMIT 10 ;



-- 1-3. 统计每天报名量
/*
字段：deleted，是否被删除，默认为0（false）
	select deleted from hive.edu_hudi.tbl_customer_relationship  group by deleted ;
字段：payment_state，支付状态
	select payment_state from hive.edu_hudi.tbl_customer_relationship group by payment_state;
*/
WITH tmp AS (
    SELECT
        format_datetime(from_unixtime(cast(payment_time as bigint) / 1000),'yyyy-MM-dd')AS day_value, customer_id
    FROM hive.edu_hudi.tbl_customer_relationship
    WHERE
            day_str = '2022-05-17' AND payment_time IS NOT NULL AND payment_state = 'PAID' AND deleted = 'false'
)
SELECT day_value, COUNT(customer_id) AS total FROM tmp GROUP BY day_value ;


-- 1-4. 将结果保存MySQL数据库表中
INSERT INTO mysql.itcast_rpt.stu_apply (report_date, report_total)
SELECT day_value, total FROM (
    SELECT day_value, COUNT(customer_id) AS total FROM (
        SELECT
            format_datetime(from_unixtime(cast(payment_time as bigint) / 1000), 'yyyy-MM-dd')AS day_value, customer_id
        FROM hive.edu_hudi.tbl_customer_relationship
        WHERE day_str = '2022-05-17' AND payment_time IS NOT NULL AND payment_state = 'PAID' AND deleted = 'false'
    ) GROUP BY day_value
) ;


select * from mysql.itcast_rpt.stu_apply;

-- 1-5. 截止昨日, 当月每日报名量统计
/*
select substr(cast(current_date AS varchar), 1, 7) AS date_val;
	  _col0
	---------
	 2021-11
*/
WITH tmp AS (
    SELECT
        format_datetime(from_unixtime(cast(payment_time as bigint) / 1000),'yyyy-MM-dd')AS day_value, customer_id
    FROM hive.edu_hudi.tbl_customer_relationship
    WHERE
        day_str = '2022-05-17' AND payment_time is not null
      and payment_state = 'PAID' AND deleted = 'false'
)
SELECT
    day_value, COUNT(customer_id) AS total
FROM tmp GROUP BY day_value
    WHERE
    day_value < substr(cast(current_date AS varchar), 1, 10)
    AND
    substr(day_value, 1, 7) = substr(cast(current_date AS varchar), 1, 7) ;



-- =======================================================================================================
-- ================================ 2. 每日访问量 =================================
-- =======================================================================================================

-- itcast_rpt.web_pv definition
CREATE TABLE  IF NOT EXISTS `itcast_rpt`.`web_pv` (
  `report_date` longtext,
  `report_total` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 1. 涉及业务字段值
select id, create_time from hive.edu_hudi.tbl_web_chat_ems WHERE day_str = '2022-05-17' limit 10;
/*
   id   |  create_time
--------+---------------
 211197 | 1564012860000
 3640   | 1562021220000
 3638   | 1562021220000
 3639   | 1562021220000
 3630   | 1562021220000
*/

-- 2. 统计每日PV
WITH tmp AS (
    SELECT
        id, format_datetime(from_unixtime(cast(create_time as bigint) / 1000), 'yyyy-MM-dd') AS day_value
    FROM hive.edu_hudi.tbl_web_chat_ems
    WHERE day_str = '2022-05-17'
)
SELECT day_value, COUNT(id) AS total FROM tmp GROUP BY day_value ;
/*
	 day_value  | total
	------------+-------
	 2019-07-26 | 52130
	 2019-07-02 | 52131
	 2019-07-01 | 53468
	 2019-07-25 | 53468
*/

-- 3. 保存MySQL数据库
INSERT INTO mysql.itcast_rpt.web_pv (report_date, report_total)
SELECT day_value, COUNT(id) AS total FROM (
  SELECT
      id, format_datetime(from_unixtime(cast(create_time as bigint) / 1000), 'yyyy-MM-dd') AS day_value
  FROM hive.edu_hudi.tbl_web_chat_ems
  WHERE day_str = '2022-05-17'
) GROUP BY day_value ;

select * from itcast_rpt.web_pv;

-- =======================================================================================================
-- ================================ 3. 每日意向数 =================================
-- =======================================================================================================

-- itcast_rpt.stu_intention definition
CREATE TABLE  IF NOT EXISTS `itcast_rpt`.`stu_intention` (
     `report_date` longtext,
     `report_total` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 1. 涉及业务字段值
select id, create_date_time, deleted from hive.edu_hudi.tbl_customer_relationship WHERE day_str = '2022-05-17' limit 10;
/*
	  id  | create_date_time | deleted
	------+------------------+---------
	 3640 | 1325940447000    | false
	 3638 | 1325937285000    | false
	 3630 | 1325871034000    | false
*/

-- 2. 统计每日意向数
WITH tmp AS (
    SELECT
        id, format_datetime(from_unixtime(cast(create_date_time as bigint) / 1000), 'yyyy-MM-dd')AS day_value
    FROM hive.edu_hudi.tbl_customer_relationship
    WHERE day_str = '2022-05-17' AND create_date_time IS NOT NULL AND deleted = 'false'
)
SELECT day_value, COUNT(id) AS total FROM tmp GROUP BY day_value ;
/*
	day_value  | total
	------------+-------
	 2012-01-08 |     6
	 2015-07-12 |    37
	 2015-01-30 |    52
	 2014-03-04 |    43
	 2012-01-30 |    11
*/

-- 3. 保存MySQL数据库
INSERT INTO mysql.itcast_rpt.stu_intention (report_date, report_total)
SELECT day_value, COUNT(id) AS total FROM (
  SELECT
      id, format_datetime(from_unixtime(cast(create_date_time as bigint) / 1000), 'yyyy-MM-dd')AS day_value
  FROM hive.edu_hudi.tbl_customer_relationship
  WHERE day_str = '2022-05-17' AND create_date_time IS NOT NULL AND deleted = 'false'
) GROUP BY day_value ;


-- =======================================================================================================
-- ================================ 4. 每日线索量 =================================
-- =======================================================================================================

-- itcast_rpt.stu_clue definition
CREATE TABLE IF NOT EXISTS `itcast_rpt`.`stu_clue` (
   `report_date` longtext,
   `report_total` bigint(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 1. 涉及业务字段值
select id, create_date_time, clue_state, deleted from hive.edu_hudi.tbl_customer_clue WHERE day_str = '2022-05-17' limit 10;
/*
	  id  | create_date_time |       clue_state        | deleted
	------+------------------+-------------------------+---------
	 3640 | 1572913503000    | INVALID_PUBLIC_OLD_CLUE | false
	 3638 | 1572913503000    | INVALID_PUBLIC_OLD_CLUE | false
	 3639 | 1572913503000    | INVALID_PUBLIC_OLD_CLUE | false
*/

-- 2. 统计每日线索量
WITH tmp AS (
    SELECT
        id, format_datetime(from_unixtime(cast(create_date_time as bigint) / 1000), 'yyyy-MM-dd')AS day_value
    FROM hive.edu_hudi.tbl_customer_clue
    WHERE day_str = '2022-05-17' AND clue_state IS NOT NULL AND deleted = 'false'
)
SELECT day_value, COUNT(id) AS total FROM tmp GROUP BY day_value ;
/*
	day_value  | total
	 2019-04-02 |     1
	 2018-11-17 |     1
	 2019-07-31 |     1
	 2019-01-11 |     1
	 2019-10-17 |     1
*/

-- 3. 保存MySQL数据库
INSERT INTO mysql.itcast_rpt.stu_clue (report_date, report_total)
SELECT day_value, COUNT(id) AS total FROM (
  SELECT
      id, format_datetime(from_unixtime(cast(create_date_time as bigint) / 1000), 'yyyy-MM-dd')AS day_value
  FROM hive.edu_hudi.tbl_customer_clue
  WHERE day_str = '2022-05-17' AND clue_state IS NOT NULL AND deleted = 'false'
) GROUP BY day_value ;

