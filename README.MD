# selfDB 
***
该项目代码和实现主要参照于
> https://github.com/CN-GuoZiyang/MYDB

selfDB 是一个用Java实现的简单的数据库。实现了以下功能：
- 数据的可靠性和数据恢复
- 两段锁协议（2PL）实现可串行化调度
- MVCC
- 两种事务隔离级别（读提交和可重复读）
- 死锁处理
- 简单的表和字段管理
- 简陋的SQL解析
- 采用C/S模式

# 运行环境、工具
***
- JDK17
- Maven 3.8.6
- intelliJ IDEA Ultimate 2023.1

# 运行方式
***
- 首先编译源码：
    ```shell
    mvn compile
    ```
- 再运行命令来创建数据库：
    ```shell
    mvn exec:java -Dexec.mainClass="org.yujiabin.selfDB.ServerLauncher" -Dexec.args="-create /tmp/selfDB"
    ```
- 之后运行命令来打开数据库：
    ```shell
    mvn exec:java -Dexec.mainClass="org.yujiabin.selfDB.ServerLauncher" -Dexec.args="-open /tmp/selfDB"
    ```
- 最后新打开一个终端执行以下命令来运行客户端：
    ```shell
    mvn compile
    mvn exec:java -Dexec.mainClass="org.yujiabin.selfDB.ClientLauncher"
    ```
- 运行成功后，会在客户端启动交互式命令行，可在此处写SQL语句，按下回车则执行该语句，并输出运行结果。 该数据库目前支持以下SQL语句：
  - begin
    ```text
    # begin [isolation level (read committed,repeatable read)]
    begin isolation level read committed
    ```
  - commit
    ```text
    commit
    ```
  - abort
    ```text
    abort
    ```
  - create
    ```text
     # create table {table name}
     # {field name} {field type}
     # {field name} {field type}
     # ...
     # {field name} {field type}
     # [(index {field name list})]
     createBtFile table students
     id int32,
     name string,
     age int32,
     (index id name)
    ```
  - drop
    ```text
    # drop table {table name}
    drop table students
    ```

  - select
    ```text
    # select (*{field name list}) from {table name} [{where statement}]
    select * from student where id = 1
    select name from student where id > 1 and id < 4
    select name, age, id from student where id = 12
    ```
  - insert
    ```text
    # insert into {table name} values {value list}
    insert into student values 5 "connery" 22
    ```

  - delete
    ```text
    # delete from {table name} {where statement}
    delete from student where name = "connery"
    ```

  - update
    ```text
    # update {table name} set {field name}={value} [{where statement}]
    update student set name = "hans" where id = 5
    ```
  - where
    ```text
    # where {field name} (><=) {value} [(andor) {field name} (><=) {value}]
    where age > 10 or age < 3
    ```
  
  - quit
    ```text
    quit
    ```