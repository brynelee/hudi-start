show master logs;

create database test ;

show databases ;

GRANT ALL PRIVILEGES ON test.* TO 'hadoop'@'%' WITH GRANT OPTION;
flush privileges;

create database if not exists 7mo;

show databases ;

GRANT ALL PRIVILEGES ON 7mo.* TO 'hadoop'@'%' WITH GRANT OPTION;
flush privileges;