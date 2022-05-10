-- hive HQL operation demo on 王者荣耀 data

use oldlu;

create table t_hot_hero_skin_price(
   id int,
   name string,
   win_rate int,
   skin_price map<string,int>
)
row format delimited
fields terminated by ','
collection items terminated by '-'
map keys terminated by ':' ;

