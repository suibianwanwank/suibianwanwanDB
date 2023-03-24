# SuibianwanwanDB

​	*SuibianwanwanDB* is a simple relational database written in java. Most of the implementation refers to the implementation principle of mysql, and realizes the basic functions of mysql.

## Project Architecture

![suibianwanwanDb](
https://suibianwanwan.oss-cn-hangzhou.aliyuncs.com/suibianwanwan.png)

## Overview

### *Already*

* Complete data recovery caused by abnormal downtime through redo logs
* mvcc version chain realizes snapshot read and transaction rollback
* Optimized LRU algorithm, page replacement through hot and cold chain
* Use netty to realize simple http communication (easy access)
* B+ tree and gap lock
* mysql basic lexical analysis
* Implemented read committed, read uncommitted, repeatable read three transaction isolation levels
* deadlock detection

### *Future*

* Optimizing sql syntax parsing
* Adding null lists and variable-length field lists
* Adding Clustered Indexes



## Quick start

First clone the file and run it, if it is not jdk17, modify the compilation properties in the pom file,If there is a need for port, page size, data path, etc., you can configure it in the yml file.

![image-20230325005820100](
https://suibianwanwan.oss-cn-hangzhou.aliyuncs.com/1.png
)

When you see the following code, it means that the http service starts successfully

![image-20230325005944508](
https://suibianwanwan.oss-cn-hangzhou.aliyuncs.com/2.png)

Use json format to send post requests to write sql statements in sql to execute commands

![image-20230325010531959](
https://suibianwanwan.oss-cn-hangzhou.aliyuncs.com/3.png)



## Contributions

suibianwanwan
