--创建数据库并切换使用
create database oldlu;
use oldlu;
--ddl create table
create table oldlu.t_archer(
   id int comment "ID",
   name string comment "英雄名称",
   hp_max int comment "最大生命",
   mp_max int comment "最大法力",
   attack_max int comment "最高物攻",
   defense_max int comment "最大物防",
   attack_range string comment "攻击范围",
   role_main string comment "主要定位",
   role_assist string comment "次要定位"
) comment "王者荣耀射手信息"
row format delimited fields terminated by "\t";

