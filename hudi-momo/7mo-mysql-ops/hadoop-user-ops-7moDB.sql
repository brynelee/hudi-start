show databases ;

CREATE TABLE IF NOT EXISTS 7mo.7mo_report (
    7mo_name varchar(100) not null ,
    7mo_total bigint(20) not null ,
    7mo_category varchar(100) not null ,
    primary key (7mo_name, 7mo_category)
)engine=InnoDB default charset = utf8mb4;

use 7mo;

show tables;

drop table 7mo_report;



