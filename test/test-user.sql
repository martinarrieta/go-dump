# Create Testuser
CREATE USER 'testuser'@'localhost' IDENTIFIED BY 'simpletest';
GRANT SELECT, LOCK TABLES, RELOAD ON *.* TO 'testuser'@'localhost';
