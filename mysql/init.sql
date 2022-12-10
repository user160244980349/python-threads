CREATE USER user1 IDENTIFIED BY 'secret';
CREATE DATABASE test_database;
GRANT ALL PRIVILEGES ON test_database.* TO user1;
USE test_database;

CREATE TABLE `test_table` (
    `id`   INT PRIMARY KEY AUTO_INCREMENT,
    `f1` VARCHAR(255),
    `f2` VARCHAR(255),
    `f3` VARCHAR(255),
    `f4` VARCHAR(255),
    `f5` VARCHAR(255),
    `f6` VARCHAR(255),
    `f7` VARCHAR(255)
);