package org.apache.spark.sql.hive

import org.apache.hadoop.mapred._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.SerializableConfiguration


class HadoopPartitionRDD[K, V](
    @transient sqlc: SQLContext,
    broadcastedConf: Broadcast[SerializableConfiguration],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatIns: InputFormat[K, V],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int) extends HadoopRDD[K, V](sqlc.sparkContext,
    broadcastedConf,
    initLocalJobConfFuncOpt,
    null,
    keyClass,
    valueClass,
    minPartitions
) {

  override protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    inputFormatIns
  }
}
