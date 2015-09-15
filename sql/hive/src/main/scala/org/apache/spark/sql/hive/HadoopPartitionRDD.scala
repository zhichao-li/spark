package org.apache.spark.sql.hive

import java.io.EOFException
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hive.common.util.ShutdownHookManager
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.{SerializableWritable, InterruptibleIterator, TaskContext, Partition}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mapred.{CombineFileRecordReader, CurrentPath, CombineSparkInputFormat}
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.{NextIterator}


class HadoopPartitionRDD[K, V](
    @transient sqlc: SQLContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int,
    mapperSplitSize: Int) extends HadoopRDD[K, V](sqlc.sparkContext,
    broadcastedConf,
    initLocalJobConfFuncOpt,
    inputFormatClass,
    keyClass,
    valueClass,
    minPartitions
) {

  override protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
      val inputFormat = super.getInputFormat(conf)
      new CombineSparkInputFormat(inputFormat, mapperSplitSize)
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)
      val jobConf = getJobConf()

      val inputMetrics = context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Hadoop)

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = inputMetrics.bytesReadCallback.orElse {
        split.inputSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
          case _ => None
        }
      }
      inputMetrics.setBytesReadCallback(bytesReadCallback)

      var reader: CombineFileRecordReader[K, V] = null
      val inputFormat = getInputFormat(jobConf)
      HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format(new Date()),
        context.stageId, theSplit.index, context.attemptNumber, jobConf)
      reader = inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL).asInstanceOf[CombineFileRecordReader[K, V]]

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener {context => closeIfNeeded()}
      val key: K = reader.createKey()
      val value: V = reader.createValue()
      var currentPath = new CurrentPath()

      override def getNext(): (K, V) = {
        try {
          finished = !reader.next(key, value, currentPath)
        } catch {
          case eof: EOFException =>
            finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        (new Text(currentPath.getPath).asInstanceOf[K], value)
      }

      override def close() {
        try {
          reader.close()
          if (bytesReadCallback.isDefined) {
            inputMetrics.updateBytesRead()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit] ||
            split.inputSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.inputSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        } catch {
          case e: Exception => {
//            if (!ShutdownHookManager.inShutdown()) {
//              logWarning("Exception in RecordReader.close()", e)
//            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }
}
