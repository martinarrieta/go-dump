[![Build Status](https://travis-ci.org/martinarrieta/go-dump.svg?branch=master)](https://travis-ci.org/martinarrieta/go-dump)

# go-dump

** IMPORTANT! the project is under development and there is a lot of work to do. **

This is a parallel MySQL dump in Go.

You can either dump a database or multiple databases or a table or multiple tables.

## How it works?

go-dump uses the [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) capabilities from the storage engines (currently only [InnoDB](https://dev.mysql.com/doc/refman/5.7/en/innodb-multi-versioning.html)) to get a consistent state of a backup. The concept is very similar to [mydumper](https://github.com/maxbube/mydumper) but with some extra features that I found useful.

## Pending

* Store the slave position if to get the master information. AKA [--dump-slave](https://dev.mysql.com/doc/refman/5.7/en/mysqldump.html#option_mysqldump_dump-slave) from mysqldump.
* Make tests
* Make more tests

## Usage

The parameters and options are listed here:

```
# go-dump --help
  -add-drop-table
    	Add drop table before create table.
  -all-databases
    	Dump all the databases.
  -channel-buffer-size int
    	Task channel buffer size (default 1000)
  -chunk-size int
    	Chunk size to get the rows (default 1000)
  -databases string
    	List of comma separated databases to dump
  -debug
    	Display debug information.
  -destination string
    	Directory to store the dumps
  -dry-run
    	Just calculate the number of chaunks per table and display it.
  -execute
    	Execute the dump.
  -help
    	Display this message
  -host string
    	MySQL hostname (default "localhost")
  -lock-tables
    	Lock tables to get consistent backup (default true)
  -master-data
    	Get the master data. (default true)
  -mysql-user string
    	MySQL user name
  -output-chunk-size int
    	Chunk size to output the rows
  -password string
    	MySQL password
  -port int
    	MySQL port number (default 3306)
  -skip-use-database
    	Skip USE "database" in the dump.
  -socket string
    	MySQL socket file
  -tables string
    	List of comma separated tables to dump.
    	Each table should have the database name included, for example "mydb.mytable,mydb2.mytable2"
  -tables-without-uniquekey string
    	Action to have with tables without any primary or unique key.
    	Valid actions are: 'error', 'skip', 'single-chunk'. (default "error")
  -threads int
    	Number of threads to use (default 1)
  -version
    	Display version and exit
```

## Examples

```go-dump --threads 8 --chunk-size 50000 --output-chunk-size 1000  --channel-buffer-size  2000 --tables-without-uniquekey "single-chunk" --add-drop-table  --databases "test" --mysql-user root --destination /tmp/testbackup   --execute   --skip-use-database ```

Explanation of this command:

This command will execute 8 threads, it will create chunks of 50000 rows and it will write in chunks of 1000 rows, the buffer for the chunks it will be 2000 and the tables without a primary or unique key will be done in a single chunk. It will add the drop table command and the database that it will backup it is "test". The user to connect to the mysql database is "root" and the dastination directory is "/tmp/testbackup". We want to execute the backup and we don't want to add the "USE DATABASE" command on each file.

## Focus of this project

The main focus of this project is to be able to make and restore consistent logical backups from MySQL.
