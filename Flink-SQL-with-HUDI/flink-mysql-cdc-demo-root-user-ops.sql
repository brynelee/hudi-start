show master logs;

create database test ;

show databases ;

GRANT ALL PRIVILEGES ON test.* TO 'hadoop'@'%' WITH GRANT OPTION;
flush privileges;