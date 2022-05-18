CREATE DATABASE IF NOT EXISTS itcast_nev;

GRANT ALL PRIVILEGES ON itcast_nev.* TO 'hadoop'@'%' WITH GRANT OPTION;
flush privileges;

show databases ;

show master logs;