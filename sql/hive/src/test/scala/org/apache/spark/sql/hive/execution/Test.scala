package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Test {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test partitions")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val tableName = "myparts_small1"
    val partitionNum = 100
    val items = 1000
//    createTable(tableName, partitionNum, items)

    queryTable(tableName)

    def queryTable(tableName: String) = {
      var start = System.currentTimeMillis()
      sqlContext.sql("set spark.sql.mapper.splitSize=-1")
      println(sqlContext.sql(s"select * from $tableName").collect.length + " length")
      var stop = System.currentTimeMillis()
      println((stop - start) / 1000)
    }

    def createTable(tableName: String, partitionNum: Int, item: Int) = {
      sqlContext.sql(s"drop table if exists $tableName")
      sqlContext.sql(s"create table $tableName (a int, b int, c int) partitioned by (d string, e int) stored as  sequencefile") //stored as parquetfile  orc sequencefile")
      sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      sqlContext.sql("set hive.exec.max.dynamic.partitions=10000")
      val df = (1 to item).map { i => (i, i, i, i)}.toDF("a", "b", "c", "d")
      //df.saveAsTable("mydf")
      df.registerTempTable("mydf")
      sqlContext.sql(s"insert into table $tableName partition(d='ee', e) select a, b, c, d  % $partitionNum as e from mydf")
    }
  }
}
