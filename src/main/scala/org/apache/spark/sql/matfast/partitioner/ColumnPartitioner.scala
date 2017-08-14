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

package org.apache.spark.sql.matfast.partitioner

import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.matfast.util.MatfastSerializer


class ColumnPartitioner(partitions: Int) extends Partitioner{

  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions")

  override val numPartitions = partitions

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int, j: Int) => j % partitions
      case (i: Int, j: Int, _: Int) => j % partitions
      case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[ColumnPartitioner] &&
      numPartitions == other.asInstanceOf[ColumnPartitioner].numPartitions
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(partitions: java.lang.Integer)
  }
}

object ColumnPartitioner {

  def apply(origin: RDD[InternalRow], numPartitions: Int): RDD[((Int, Int), InternalRow)] = {
    val rdd = origin.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = row.getStruct(2, 7)
      ((rid, cid), matrix)
    }
    val partitioner = new ColumnPartitioner(numPartitions)
    val shuffled = new ShuffledRDD[(Int, Int), InternalRow, InternalRow](rdd, partitioner)
    shuffled.setSerializer(new MatfastSerializer(new SparkConf(false)))
    shuffled
  }
}
