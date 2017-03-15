package org.apache.spark.sql.matfast.partitioner

import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.matfast.util.MatfastSerializer


/**
  * Created by yongyangyu on 3/13/17.
  */
class RowPartitioner(partitions: Int) extends Partitioner{

  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions")

  override def numPartitions = partitions

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int, j: Int) => i % partitions
      case (i: Int, j: Int, _: Int) => i % partitions
      case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[RowPartitioner] &&
      numPartitions == other.asInstanceOf[RowPartitioner].numPartitions
  }
}

object RowPartitioner {

  def apply(origin: RDD[InternalRow], numPartitions: Int): RDD[((Int, Int), InternalRow)] = {
    val rdd = origin.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = row.getStruct(2, 7)
      ((rid, cid), matrix)
    }
    val partitioner = new RowPartitioner(numPartitions)
    val shuffled = new ShuffledRDD[(Int, Int), InternalRow, InternalRow](rdd, partitioner)
    shuffled.setSerializer(new MatfastSerializer(new SparkConf(false)))
    shuffled
  }
}
















