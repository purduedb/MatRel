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

package org.apache.spark.sql.matfast.matrix

import org.apache.spark.mllib.linalg.distributed.{MatrixEntry}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.matfast.partitioner.GridPartitioner

class CooMatrix (val entries: RDD[MatrixEntry],
                 private var nRows: Long,
                 private var nCols: Long
                ) {
  def this(entries: RDD[MatrixEntry]) = this(entries, 0L, 0L)

  def numCols(): Long = {
    if (nCols <= 0L) {
      computeSize()
    }
    nCols
  }

  def numRows(): Long = {
    if (nRows <= 0L) {
      computeSize()
    }
    nRows
  }

  def toBlockMatrixRDD(blkSize: Int): RDD[((Int, Int), MLMatrix)] = {
    require(blkSize > 0,
      s"blkSize needs to be greater than 0. blkSize: $blkSize")
    val m = numRows()
    val n = numCols()
    val numRowBlocks = math.ceil(m.toDouble / blkSize).toInt
    val numColBlocks = math.ceil(n.toDouble / blkSize).toInt
    val partitioner = GridPartitioner(numRowBlocks, numColBlocks, entries.partitions.length)
    val blocks = entries.map { entry =>
      val blockRowIndex = (entry.i / blkSize).toInt
      val blockColIndex = (entry.j / blkSize).toInt

      val rowId = entry.i % blkSize
      val colId = entry.j % blkSize

      ((blockRowIndex, blockColIndex), (rowId.toInt, colId.toInt, entry.value))
    }.groupByKey(partitioner).map { case ((blockRowIndex, blockColIndex), entry) =>
      val effRows = math.min(m - blockRowIndex.toLong * blkSize, blkSize).toInt
      val effCols = math.min(n - blockColIndex.toLong * blkSize, blkSize).toInt
      ((blockRowIndex, blockColIndex), SparseMatrix.fromCOO(effRows, effCols, entry).asInstanceOf[MLMatrix])
    }
    blocks
  }

  private def computeSize(): Unit = {
    val (m1, n1) = entries.map(entry => (entry.i, entry.j)).reduce { case ((i1, j1), (i2, j2)) =>
      (math.max(i1, i2), math.max(j1, j2))
    }
    nRows = math.max(nRows, m1 + 1L)
    nCols = math.max(nCols, n1 + 1L)
  }

}
