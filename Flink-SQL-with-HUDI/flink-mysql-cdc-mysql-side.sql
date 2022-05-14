show databases ;

use test;

create table test.tbl_users(
   id bigint auto_increment primary key,
   name varchar(20) null,
   birthday timestamp default CURRENT_TIMESTAMP not null,
   ts timestamp default CURRENT_TIMESTAMP not null
);

show tables ;
select * from tbl_users;

INSERT INTO test.tbl_users(name) VALUES('itheima') ;

insert into test.tbl_users (name) values ('zhangsan')
insert into test.tbl_users (name) values ('lisi');
insert into test.tbl_users (name) values ('wangwu');
insert into test.tbl_users (name) values ('laoda');
insert into test.tbl_users (name) values ('laoer');

-- 创建CDC表以后继续插入

insert into test.tbl_users (name) values ('Tom')
insert into test.tbl_users (name) values ('Lisa');
insert into test.tbl_users (name) values ('Wangning');
insert into test.tbl_users (name) values ('laowu');
insert into test.tbl_users (name) values ('laoliu');

-- 创建hudi表以后继续插入

insert into test.tbl_users (name) values ('Brain')
insert into test.tbl_users (name) values ('Shally');
insert into test.tbl_users (name) values ('Louzong');
insert into test.tbl_users (name) values ('Shenlong');
insert into test.tbl_users (name) values ('jianming');

-- 创建hive（hudi并表）之后继续插入

insert into test.tbl_users (name) values ('lao11')
insert into test.tbl_users (name) values ('lao12');
insert into test.tbl_users (name) values ('lao13');
insert into test.tbl_users (name) values ('lao14');
insert into test.tbl_users (name) values ('lao15');

insert into test.tbl_users (name) values ('lao16');
insert into test.tbl_users (name) values ('lao17');
insert into test.tbl_users (name) values ('lao18');

