package org.apache.spark.sql.matfast.partitioner

import org.apache.spark.Partitioner

/**
  * Created by yongyangyu on 3/17/17.
  */
class IndexPartitioner(partitions: Int) extends Partitioner{

  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int) => i
      case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[IndexPartitioner] &&
      numPartitions == other.asInstanceOf[IndexPartitioner].numPartitions
  }
}
