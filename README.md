# hbase-sql
v0.1.0 2013-1-9

Automatically exported from http://code.google.com/p/hbase-sql

通过sql来查询hbase上的数据

## 如何简化从hbase中查询数据
为了兼容以前从关系型数据库中查询数据的接口, 让hbase可以通过sql语句来查询其中的数据.

hive有这样的功能, 他支持通过类似sql语句的语法来操作hbase中的数据, 但是速度太慢了, 因为hive本身就不是用来查询数据的, hive是数据仓库, 做数据分析的, 不适合我们的应用场景.

hbase本身提供的api中, 只有scan是用来查询数据的, 因此我们需要将sql语句转成scan 参考<<[利用hbase的coprocessor机制来在hbase上增加sql解析引擎–(一)原因&架构](http://blog.hummingbird-one.com/?p=10196)>>发现是可行的

因此总体架构为

```
sql语句 --sql解析器--> sql语法节点(对象) -> scan -> hbase -> ResultScanner -> List<DynaBean>
```

例如一个简单的sql语句
```
select a, b from table1 where a = 1 and b = 2
```
我们通过sql解析器可以得到sql语句的各个部分, 再调用hbase api中相应的语句来达到相同的效果

```java
// 要查询的表
HTable table = new HTable(conf, "table1");
// 要查询的字段
Scan scan = new Scan();
scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"));
scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"));
// where条件
// a = 1
SingleColumnValueFilter a = new SingleColumnValueFilter(Bytes.toBytes("cf"),
        Bytes.toBytes("a"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(1)));
filterList.addFilter(filter);
// b = 2
SingleColumnValueFilter b = new SingleColumnValueFilter(Bytes.toBytes("cf"),
        Bytes.toBytes("b"), CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(2)));
// and
FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, a, b);
scan.setFilter(filterList);
```

## 目前支持的功能
具体细节请参考单元测试

1. 从oracle数据库中导入表数据到hbase
```java
OracleDataLoader.loadTable("TABLE_NAME", new String[] { "PK_COLUMN_NAME" });
```
2. 通过SQL语句来查询hbase中的表数据
```java
List<DynaBean> rows = HbaseQuery.select("SQL");
```
目前支持的SQL语句

```sql
SELECT * FROM report1                       /* 查询所有数据 */
SELECT A, B FROM report1                    /* 只查询某些列 */
SELECT * FROM report1 WHERE A = 1 and B = 2 /* 过滤条件只能是AND逻辑, 而且是等于关系 */
SELECT * FROM report1 limit 3 offset 2      /* 分页 */
```

## 如何使用
1. 在Download中下载最新版的hbase-sql.jar, 将其放在lib中.

        注意项目lib的依赖
        * commons-beanutils-core-1.8.0.jar
        * commons-configuration-1.6.jar
        * commons-dbutils-1.5.jar
        * commons-lang-2.5.jar
        * commons-logging-1.1.1.jar
        * hadoop-core-1.0.4.jar
        * hbase-0.94.3.jar
        * jsqlparser-0.7.0.jar
        * log4j-1.2.16.jar
        * ojdbc14-10.2.0.5.jar
        * protobuf-java-2.4.0a.jar
        * slf4j-api-1.4.3.jar
        * slf4j-log4j12-1.4.3.jar
        * zookeeper-3.4.3.jar

2. 在项目的src中配置好hbase-site.xml, 否则无法连接到hbase来体验hbase-sql的功能

3. 测试
```java
List<DynaBean> rows = new HbaseQueryImpl().select("select * from report1");
System.out.println(rows.size());
```

## TODO
支持更复杂的SQL查询语句
