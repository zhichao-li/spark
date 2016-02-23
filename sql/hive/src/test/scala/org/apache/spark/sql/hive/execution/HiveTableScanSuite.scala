/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.util.Utils

class HiveTableScanSuite extends HiveComparisonTest {

  createQueryTest("partition_based_table_scan_with_different_serde",
    """
      |CREATE TABLE part_scan_test (key STRING, value STRING) PARTITIONED BY (ds STRING)
      |ROW FORMAT SERDE
      |'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
      |STORED AS RCFILE;
      |
      |FROM src
      |INSERT INTO TABLE part_scan_test PARTITION (ds='2010-01-01')
      |SELECT 100,100 LIMIT 1;
      |
      |ALTER TABLE part_scan_test SET SERDE
      |'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
      |
      |FROM src INSERT INTO TABLE part_scan_test PARTITION (ds='2010-01-02')
      |SELECT 200,200 LIMIT 1;
      |
      |SELECT * from part_scan_test;
    """.stripMargin)

  // In unit test, kv1.txt is a small file and will be loaded as table src
  // Since the small file will be considered as a single split, we assume
  // Hive / SparkSQL HQL has the same output even for SORT BY
  createQueryTest("file_split_for_small_table",
    """
      |SELECT key, value FROM src SORT BY key, value
    """.stripMargin)

  test("Spark-4041: lowercase issue") {
    TestHive.sql("CREATE TABLE tb (KEY INT, VALUE STRING) STORED AS ORC")
    TestHive.sql("insert into table tb select key, value from src")
    TestHive.sql("select KEY from tb where VALUE='just_for_test' limit 5").collect()
    TestHive.sql("drop table tb")
  }

  test("Spark-4077: timestamp query for null value") {
    TestHive.sql("DROP TABLE IF EXISTS timestamp_query_null")
    TestHive.sql(
      """
        CREATE EXTERNAL TABLE timestamp_query_null (time TIMESTAMP,id INT)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
      """.stripMargin)
    val location =
      Utils.getSparkClassLoader.getResource("data/files/issue-4077-data.txt").getFile()

    TestHive.sql(s"LOAD DATA LOCAL INPATH '$location' INTO TABLE timestamp_query_null")
    assert(TestHive.sql("SELECT time from timestamp_query_null limit 2").collect()
      === Array(Row(java.sql.Timestamp.valueOf("2014-12-11 00:00:00")), Row(null)))
    TestHive.sql("DROP TABLE timestamp_query_null")
  }

  test("Spark-4959 Attributes are case sensitive when using a select query from a projection") {
    sql("create table spark_4959 (col1 string)")
    sql("""insert into table spark_4959 select "hi" from src limit 1""")
    table("spark_4959").select(
      'col1.as("CaseSensitiveColName"),
      'col1.as("CaseSensitiveColName2")).registerTempTable("spark_4959_2")

    assert(sql("select CaseSensitiveColName from spark_4959_2").head() === Row("hi"))
    assert(sql("select casesensitivecolname from spark_4959_2").head() === Row("hi"))
  }

  test("combine small files") {
    def getPartitions[T](sc: SparkContext, rdd: RDD[T]): Array[Long] = {
      sc.runJob(rdd, (x: Iterator[T]) => 1)
    }
    val partitionNum = 5
    val partitionTable = "combine_small"
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sql("set spark.sql.mapper.splitCombineSize=1000000")
    val df = (1 to 100).map { i => (i, i) }.toDF("a", "b").repartition(100)
    df.registerTempTable("temp")
    sql(
      s"""create table $partitionTable (a int, b string)
          |partitioned by (c int)
          |stored as orc""".stripMargin)
    sql(
      s"""insert into table $partitionTable partition(c)
          |select a, b, (b % $partitionNum) as c from temp""".stripMargin)
    val result = sql( s"""select * from $partitionTable order by a""")
    val rddPartitions = getPartitions(TestHive.sparkContext, result.rdd)
    // Ensure that there are only have 4 RDD partitions after combination
    assert(rddPartitions.length == 5)
    // Ensure that the result is the same as the original after combination
    assert(
      result.collect().map(_.toString()).deep
      ==  (1 to 100).map{i => s"[$i,$i,${i % partitionNum}]"}.toArray.deep
    )
  }
}
