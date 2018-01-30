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

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class RowMatrix(val rows: RDD[String]) extends Serializable {

  private def read(line: String): Array[Double] = {
    val elems = line.split(",")
    val res = new Array[Double](elems.length)
    for (i <- 0 until res.length) {
      res(i) = elems(i).toDouble
    }
    res
  }

  def toBlockMatrixRDD(blkSize: Int): (Long, RDD[((Int, Int), MLMatrix)]) = {
    val colNum = rows.first().trim().split(",").length - 1
    val rdd0 = rows.map { line =>
      val rowId = line.trim().split(",")(0).toLong - 1
      ((rowId / blkSize).toInt, (rowId.toLong, line.trim().substring((rowId + 1).toString.length + 1)))
    }
    val rowNum = rdd0.count()
    val rdd = rdd0.groupByKey()
      .flatMap { case (rowBlkId, iter) =>
          val numColBlks = math.ceil(colNum * 1.0 / blkSize).toInt
          val arrs = Array.ofDim[Array[Double]](numColBlks)
          val currRow = if (rowBlkId == rowNum / blkSize) (rowNum - blkSize * rowBlkId).toInt else blkSize
          for (j <- 0 until arrs.length) {
            if (j == colNum / blkSize) {
              arrs(j) = Array.ofDim[Double]((currRow * (colNum - colNum / blkSize * blkSize)).toInt)
            } else {
              arrs(j) = Array.ofDim[Double](currRow * blkSize)
            }
          }
          for (row <- iter) {
            val rowId = row._1
            val values = read(row._2)
            for (j <- 0 until values.length) {
              val colBlkId = j / blkSize
              val localRowId = rowId - rowBlkId * blkSize
              val localColId = j - colBlkId * blkSize
              val idx = currRow * localColId + localRowId
              arrs(colBlkId)(idx.toInt) = values(j)
            }
          }
          val buffer = ArrayBuffer[((Int, Int), MLMatrix)]()
          for (j <- 0 until arrs.length) {
            if (j == colNum / blkSize) {
              buffer.append(((rowBlkId.toInt, j),
                new DenseMatrix(currRow, (colNum - colNum / blkSize * blkSize).toInt, arrs(j))))
            } else {
              buffer.append(((rowBlkId.toInt, j),
                new DenseMatrix(currRow, blkSize, arrs(j))))
            }
          }
          buffer
      }
    (colNum, rdd)
  }
}
