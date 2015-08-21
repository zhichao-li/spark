package org.apache.spark.sql.hive.execution

import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lizhichao on 8/12/15.
 */
object Test {
  

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test partitions")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
//    val tableName = "myparts_small"
//    val partitionNum = 100

//    val tableName = "myparts_big"
//    val partitionNum = 5000
//    val tableName = "myparts_10000"
//    val partitionNum = 10000
    val tableName = "ty"
    val partitionNum = 1000
    val items = 10000
//    createTable(tableName, partitionNum, items)
    
    queryTable(tableName)
//    println("total time: " + HadoopRDD.totalTime)
    Thread.sleep(1000000000)
    
    def queryTable(tableName: String) = {
          var start = System.currentTimeMillis()
          sqlContext.sql("set hive.execution.engine=spark")
          println( sqlContext.sql(s"select * from $tableName").collect.length + " length" )
          var stop = System.currentTimeMillis()
          println((stop - start)/1000)
    }

    def createTable(tableName: String, partitionNum: Int, item: Int) = {
      sqlContext.sql(s"drop table if exists $tableName")
      sqlContext.sql(s"create table $tableName (a int, b int, c int) partitioned by (d string, e int) stored as sequencefile") //stored as parquetfile")
      sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      sqlContext.sql("set hive.exec.max.dynamic.partitions=10000")
      val df = (1 to item).map { i => (i, i, i, i ) }.toDF("a", "b", "c", "d")
      //df.saveAsTable("mydf")
      df.registerTempTable("mydf")
      sqlContext.sql(s"insert into table $tableName partition(d='ee', e) select a, b, c, d  % $partitionNum as e from mydf")
    }
  }
//createTable("myparts_600_1tw", 10000, 100000000)
}
