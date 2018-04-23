-- Create Testuser
CREATE USER 'testuser'@'localhost' IDENTIFIED BY 'simpletest';
GRANT SELECT, LOCK TABLES, RELOAD, REPLICATION CLIENT ON *.* TO 'testuser'@'localhost';
