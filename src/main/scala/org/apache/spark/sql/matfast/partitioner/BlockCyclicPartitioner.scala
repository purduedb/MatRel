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


/**
 * To make load-balancing, we adopt block-cyclic distribution strategy.
 * For further information, please refer to Section 1.6 from "Matrix Computation" (4th edition)
 * by Gene Golub and Charles Van Loan.
 */

class BlockCyclicPartitioner(val ROW_BLKS: Int,
                             val COL_BLKS: Int,
                             val ROW_BLKS_PER_PARTITION: Int,
                             val COL_BLKS_PER_PARTITION: Int) extends Partitioner{

  require(ROW_BLKS > 0, s"Number of row blocks should be larger than 0, but found $ROW_BLKS")
  require(COL_BLKS > 0, s"Number of col blocks should be larger than 0, but found $COL_BLKS")
  require(ROW_BLKS_PER_PARTITION > 0,
    s"Number of row blocks per partition should be larger than 0, " +
    s"but found $ROW_BLKS_PER_PARTITION")
  require(COL_BLKS_PER_PARTITION > 0,
    s"Number of col blocks per partition should be larger than 0, " +
    s"but found $COL_BLKS_PER_PARTITION")

  private val row_partition_num = math.ceil(ROW_BLKS * 1.0 / ROW_BLKS_PER_PARTITION).toInt
  private val col_partition_num = math.ceil(COL_BLKS * 1.0 / COL_BLKS_PER_PARTITION).toInt

  private val num_row_part = ROW_BLKS / row_partition_num
  private val num_col_part = COL_BLKS / col_partition_num

  override val numPartitions: Int = row_partition_num * col_partition_num

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int, j : Int) =>
        ((i % num_row_part) * col_partition_num + (j % num_col_part)) % numPartitions
      case (i: Int, j: Int, _: Int) =>
        ((i % num_row_part) * col_partition_num + (j % num_col_part)) % numPartitions
      case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case r: BlockCyclicPartitioner =>
        (ROW_BLKS == r.ROW_BLKS) &&
          (COL_BLKS == r.COL_BLKS) &&
          (ROW_BLKS_PER_PARTITION == r.ROW_BLKS_PER_PARTITION) &&
          (COL_BLKS_PER_PARTITION == r.COL_BLKS_PER_PARTITION)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(
      ROW_BLKS: java.lang.Integer,
      COL_BLKS: java.lang.Integer,
      ROW_BLKS_PER_PARTITION: java.lang.Integer,
      COL_BLKS_PER_PARTITION: java.lang.Integer
    )
  }
}

object BlockCyclicPartitioner {

  def apply(origin: RDD[InternalRow],
            ROW_BLKS: Int,
            COL_BLKS: Int,
            ROW_BLKS_PER_PARTITION: Int,
            COL_BLKS_PER_PARTITION: Int): RDD[((Int, Int), InternalRow)] = {

    val rdd = origin.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = row.getStruct(2, 7)
      ((rid, cid), matrix)
    }
    val partitioner = new BlockCyclicPartitioner(ROW_BLKS, COL_BLKS,
      ROW_BLKS_PER_PARTITION, COL_BLKS_PER_PARTITION)
    val shuffled = new ShuffledRDD[(Int, Int), InternalRow, InternalRow](rdd, partitioner)
    shuffled.setSerializer(new MatfastSerializer(new SparkConf(false)))
    shuffled
  }
}
