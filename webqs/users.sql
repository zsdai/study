create database `webqs` default character set utf8 collate utf8_general_ci;
GRANT ALL PRIVILEGES ON webqs.* TO 'root'@'192.168.200.%' IDENTIFIED BY '123456' WITH GRANT OPTION;
flush privileges;