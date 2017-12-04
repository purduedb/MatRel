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

package org.apache.spark.sql.matfast.execution

import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GenericInternalRow, ExprId}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.matfast.matrix._
import org.apache.spark.sql.matfast.partitioner.{BlockCyclicPartitioner, RowPartitioner}
import org.apache.spark.sql.matfast.util._
import org.apache.spark.sql.types.LongType

import scala.collection.concurrent.TrieMap

case class RemoveEmptyRowsDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val removedRowIds = MatfastExecutionHelper.findEmptyRows(rootRdd)
    MatfastExecutionHelper.removeEmptyRows(rootRdd, removedRowIds)
  }
}

case class RemoveEmptyColumnsDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val removedColumnIds = MatfastExecutionHelper.findEmptyColumns(rootRdd)
    MatfastExecutionHelper.removeEmptyColumns(rootRdd, removedColumnIds)
  }
}

case class ProjectRowDirectExecution(child: SparkPlan,
                                     blkSize: Int,
                                     index: Long) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rowblkID = (index / blkSize).toInt
    val offset = (index % blkSize).toInt
    val rootRdd = child.execute()
    val rowBlks = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      (rid, (cid, MLMatrixSerializer.deserialize(matrixInternalRow)))
    }.filter(tuple => tuple._1 == rowblkID)
    val rdd = rowBlks.map { tuple =>
      val cid = tuple._2._1
      val matrix = tuple._2._2
      matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) { // stored in the column orientation
            val resRow = Array.fill(den.numCols)(0.0)
            for (i <- 0 until resRow.length)  {
              resRow(i) = den.values(offset + i * den.numRows)
            }
            val matBlk = new DenseMatrix(1, resRow.length, resRow)
            ((0, cid), matBlk.asInstanceOf[MLMatrix])
          } else { // stored in the row orientation
            val resRow = Array.fill(den.numRows)(0.0)
            for (i <- 0 until resRow.length) {
              resRow(i) = den.values(offset + i * den.numCols)
            }
            val matBlk = new DenseMatrix(1, resRow.length, resRow)
            ((0, cid), matBlk.asInstanceOf[MLMatrix])
          }
        case sp: SparseMatrix =>
          // Choosing a row in CSC is the same as choosing a column in CSR
          var resValues = ArrayBuffer[Double]()
          var resColPtrs = ArrayBuffer[Int]()
          var cnt = 0
          for (j <- 0 until sp.colPtrs.length - 1) {
            for (k <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
              if (offset == sp.rowIndices(k + sp.colPtrs(j))) {
                resValues += sp.values(k + sp.colPtrs(j))
                cnt += 1
              }
            }
            if (j == 0) {
              resColPtrs += 0
            } else {
              resColPtrs += resColPtrs(j - 1) + cnt
            }
          }
          resColPtrs += resValues.length
          val resRowIndices = Array.fill(resValues.length)(offset)
          val matBlk = new SparseMatrix(1, sp.numCols, resColPtrs.toArray,
            resRowIndices, resValues.toArray)
          ((0, cid), matBlk.asInstanceOf[MLMatrix])
        case _ =>
          throw new SparkException("Undefined matrix type in ProjectRowDirectExecute()")
      }
    }
    rdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, blk._1._1)
      res.setInt(1, blk._1._2)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class ProjectColumnDirectExecution(child: SparkPlan,
                                        blkSize: Int,
                                        index: Long) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val colblkID = (index / blkSize).toInt
    val offset = (index % blkSize).toInt
    val rootRdd = child.execute()
    val colBlks = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      (cid, (rid, MLMatrixSerializer.deserialize(matrixInternalRow)))
    }.filter(tuple => tuple._1  == colblkID)
    val rdd = colBlks.map { tuple =>
      val rid = tuple._2._1
      val matrix = tuple._2._2
      matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) { // stored in the column orientation
            val resCol = Array.fill(den.numRows)(0.0)
            for (i <- 0 until resCol.length) {
              resCol(i) = den.values(offset * den.numRows + i)
            }
            val matBlk = new DenseMatrix(resCol.length, 1, resCol)
            ((rid, 0), matBlk.asInstanceOf[MLMatrix])
          } else { // stored in the row orientation
            val resCol = Array.fill(den.numCols)(0.0)
            for (i <- 0 until resCol.length) {
              resCol(i) = den.values(offset * den.numCols + i)
            }
            val matBlk = new DenseMatrix(resCol.length, 1, resCol)
            ((rid, 0), matBlk.asInstanceOf[MLMatrix])
          }
        case sp: SparseMatrix =>
          // Choosing a column in CSC is the same as choosing a row in CSR
          var resValues = ArrayBuffer[Double]()
          var resRowIndices = ArrayBuffer[Int]()
          val resColPtrs = Array[Int](0, sp.colPtrs(offset + 1) - sp.colPtrs(offset))
          for (i <- 0 until sp.colPtrs(offset + 1) - sp.colPtrs(offset)) {
            val k = i + sp.colPtrs(offset)
            resValues += sp.values(k)
            resRowIndices += sp.rowIndices(k)
          }
          val matBlk = new SparseMatrix(sp.numRows, 1, resColPtrs,
            resRowIndices.toArray, resValues.toArray)
          ((rid, 0), matBlk.asInstanceOf[MLMatrix])
        case _ =>
          throw new SparkException("Undefined matrix type in ProjectColumnDirectExecute()")
      }
    }
    rdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, blk._1._1)
      res.setInt(1, blk._1._2)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class SelectDirectExecution(child: SparkPlan,
                                 blkSize: Int,
                                 rowIdx: Long,
                                 colIdx: Long) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rowblkID = (rowIdx / blkSize).toInt
    val colblkID = (colIdx / blkSize).toInt
    val rowOffset = (rowIdx % blkSize).toInt
    val colOffset = (colIdx % blkSize).toInt
    val rootRdd = child.execute()
    val blk = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      ((rid, cid), MLMatrixSerializer.deserialize(matrixInternalRow))
    }.filter(tuple => tuple._1 == (rowblkID, colblkID))
    val rdd = blk.map { tuple =>
      val matrix = tuple._2
      matrix match {
        case den: DenseMatrix =>
          val matBlk = new DenseMatrix(1, 1, Array(den.apply(rowOffset, colOffset)))
          ((0, 0), matBlk.asInstanceOf[MLMatrix])
        case sp: SparseMatrix =>
          val matBlk = new DenseMatrix(1, 1, Array(sp.apply(rowOffset, colOffset)))
          ((0, 0), matBlk.asInstanceOf[MLMatrix])
        case _ =>
          throw new SparkException("Undefined matrix type in SelectDirectExecute()")
      }
    }
    rdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, blk._1._1)
      res.setInt(1, blk._1._2)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class SelectValueExecution(child: SparkPlan,
                                v: Double,
                                eps: Double) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      val filtered = matrix match {
        case den: DenseMatrix =>
          val filteredValues = Array.fill[Double](den.values.length)(0.0)
          for (i <- 0 until den.values.length) {
            if (Math.abs(v - den.values(i)) <= eps) {
              filteredValues(i) = den.values(i)
            }
          }
          // Here assume only a small portion of data is selected.
          // A better solution may vary according to the percentage of nnz.
          new DenseMatrix(den.numRows, den.numCols, filteredValues, den.isTransposed).toSparse
        case sp: SparseMatrix =>
          val filteredValues = ArrayBuffer.empty[Double]
          val filteredRowIndices = ArrayBuffer.empty[Int]
          val filteredColPtrs = Array.fill[Int](sp.colPtrs.length)(0)
          var total = 0
          for (i <- 0 until sp.values.length) {
            if (Math.abs(v - sp.values(i)) <= eps) {
              total += 1
              filteredValues += v
              filteredRowIndices += sp.rowIndices(i)
              for (j <- 1 until filteredColPtrs.length) {
                breakable {
                  if (i >= sp.colPtrs(j - 1) && i < sp.colPtrs(j)) {
                    filteredColPtrs(j) += 1
                    break
                  }
                }
              }
            }
          }
          filteredColPtrs(sp.colPtrs.length - 1) = total
          for (k <- 1 until filteredColPtrs.length - 1) {
            filteredColPtrs(k) += filteredColPtrs(k - 1)
          }
          new SparseMatrix(sp.numRows, sp.numCols, filteredColPtrs, filteredRowIndices.toArray,
            filteredValues.toArray, sp.isTransposed)
        case _ =>
          throw new SparkException("Undefined matrix type in SelectValueExecution()")
      }
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(filtered))
      res
    }
  }
}

case class MatrixTransposeExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      val res = new GenericInternalRow(3)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      val matrixRow = MLMatrixSerializer.serialize(matrix.transpose)
      res.setInt(0, cid)
      res.setInt(1, rid)
      res.update(2, matrixRow)
      res
    }
  }
}

// this class computes rowSum() on a matrix input
case class RowSumDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      val colVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) { // row sum
            val m = den.numRows
            val arr = new Array[Double](m)
            val values = den.values
            for (i <- 0 until values.length) {
              arr(i % m) += values(i)
            }
            new DenseMatrix(m, 1, arr)
          } else { // column sum
            val n = den.numCols
            val arr = new Array[Double](n)
            val values = den.values
            for (i <- 0 until n) {
              for (j <- 0 until den.numRows) {
                arr(i) += values(i * den.numRows + j)
              }
            }
            new DenseMatrix(n, 1, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            val arr = new Array[Double](sp.numRows)
            for (i <- 0 until sp.rowIndices.length) {
              arr(sp.rowIndices(i)) += sp.values(i)
            }
            new DenseMatrix(sp.numRows, 1, arr)
          } else { // CSR format
            val arr = new Array[Double](sp.numCols)
            for (j <- 0 until sp.numCols) {
              for (i <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                arr(j) += sp.values(i + sp.colPtrs(j))
              }
            }
            new DenseMatrix(sp.numCols, 1, arr)
          }
        case _ => throw new SparkException("Undefined matrix type in RowSumDirectExecute()")
      }
      (rid, colVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val rid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class ColumnSumDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      val rowVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) {
            val n = den.numCols
            val arr = new Array[Double](n)
            val values = den.values
            for (i <- 0 until n) {
              for (j <- 0 until den.numRows) {
                arr(i) += values(i * den.numRows + j)
              }
            }
            new DenseMatrix(1, n, arr)
          } else {
            val m = den.numRows
            val arr = new Array[Double](m)
            val values = den.values
            for (i <- 0 until values.length) {
              arr(i % m) += values(i)
            }
            new DenseMatrix(1, m, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            val arr = new Array[Double](sp.numCols)
            for (j <- 0 until sp.numCols) {
              for (i <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                arr(j) += sp.values(i + sp.colPtrs(j))
              }
            }
            new DenseMatrix(1, sp.numCols, arr)
          } else { // CSR format
            val arr = new Array[Double](sp.numRows)
            for (i <- 0 until sp.rowIndices.length) {
              arr(sp.rowIndices(i)) += sp.values(i)
            }
            new DenseMatrix(1, sp.numRows, arr)
          }
        case _ => throw new SparkException("Undefined matrix type in ColumnSumDirectExecute()")
      }
      (cid, rowVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val cid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class SumDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val matrixInternalRow = row.getStruct(2, 7)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      val scalar = matrix match {
        case den: DenseMatrix =>
          new DenseMatrix(1, 1, Array[Double](den.values.sum))
        case sp: SparseMatrix =>
          new DenseMatrix(1, 1, Array[Double](sp.values.sum))
        case _ =>
          throw new SparkException("Undefined matrix type in SumDirectExecute()")
      }
      (0, scalar.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}


case class TraceDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      ((rid, cid), matrix)
    }
    val traceRdd = rdd.filter(tuple => tuple._1._1 == tuple._1._2).map { row =>
      val localTrace = row._2 match {
        case den: DenseMatrix =>
          val rowNum = den.numRows
          val colNum = den.numCols
          require(rowNum == colNum, s"block is not square, row_num=$rowNum, col_num=$colNum")
          val values = den.values
          // trace is invariant under the transpose operation
          // just compute in a uniform way
          var tr = 0.0
          for (j <- 0 until colNum) {
            tr += values(j * colNum + j)
          }
          val trMat = new DenseMatrix(1, 1, Array[Double](tr))
          (0, trMat.asInstanceOf[MLMatrix])
        case sp: SparseMatrix =>
          // similar to the dense case, no need to distinguish CSC or CSR format
          var tr = 0.0
          val values = sp.values
          val rowIndices = sp.rowIndices
          val colPtrs = sp.colPtrs
          for (j <- 0 until sp.numCols) {
            for (k <- 0 until colPtrs(j + 1) - colPtrs(j)) {
              if (rowIndices(k + colPtrs(j)) == j) {
                tr += values(k + colPtrs(j))
              }
            }
          }
          val trMat = new DenseMatrix(1, 1, Array[Double](tr))
          (0, trMat.asInstanceOf[MLMatrix])
        case _ =>
          throw new SparkException("Undefined matrix type in TraceDirectExecute()")
      }
      localTrace
    }.reduceByKey(LocalMatrix.add(_, _))

    traceRdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class RowNnzDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val colVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) { // row count
            val m = den.numRows
            val arr = new Array[Double](m)
            val values = den.values
            for (i <- 0 until values.length) {
              if (math.abs(values(i)) > 1e-6) {
                arr(i % m) += 1
              }
            }
            new DenseMatrix(m, 1, arr)
          } else { // column sum
            val n = den.numCols
            val arr = new Array[Double](n)
            val values = den.values
            for (i <- 0 until n) {
              for (j <- 0 until den.numRows) {
                if (math.abs(values(i * den.numRows + j)) > 1e-6) {
                  arr(i) += 1
                }
              }
            }
            new DenseMatrix(n, 1, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            val arr = new Array[Double](sp.numRows)
            for (i <- 0 until sp.rowIndices.length) {
              arr(sp.rowIndices(i)) += 1
            }
            new DenseMatrix(sp.numRows, 1, arr)
          } else { // CSR format
            val arr = new Array[Double](sp.numCols)
            for (i <- 0 until sp.numCols) {
              arr(i) = sp.colPtrs(i + 1) - sp.colPtrs(i)
            }
            new DenseMatrix(sp.numCols, 1, arr)
          }
        case _ => throw new SparkException("Undefined matrix type in RowNnzDirectExecute()")
      }
      (rid, colVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val rid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class RowNnzOnesExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val colVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) {
            val arr = Array.fill[Double](den.numRows)(den.numCols)
            new DenseMatrix(den.numRows, 1, arr)
          } else {
            val arr = Array.fill[Double](den.numCols)(den.numRows)
            new DenseMatrix(den.numCols, 1, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) {
            val arr = Array.fill[Double](sp.numRows)(sp.numCols)
            new DenseMatrix(sp.numRows, 1, arr)
          } else {
            val arr = Array.fill[Double](sp.numCols)(sp.numRows)
            new DenseMatrix(sp.numCols, 1, arr)
          }
        case _ =>
          throw new SparkException("Undefined matrix type in RowNnzOnesExecution()")
      }
      (rid, colVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val rid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class ColumnNnzDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val rowVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) {
            val n = den.numCols
            val arr = new Array[Double](n)
            val values = den.values
            for (i <- 0 until n) {
              for (j <- 0 until den.numRows) {
                if (math.abs(values(i * den.numRows + j)) > 1e-6) {
                  arr(i) += 1
                }
              }
            }
            new DenseMatrix(1, n, arr)
          } else {
            val m = den.numRows
            val arr = new Array[Double](m)
            val values = den.values
            for (i <- 0 until values.length) {
              if (math.abs(values(i)) > 1e-6) {
                arr(i % m) += 1
              }
            }
            new DenseMatrix(1, m, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            val arr = new Array[Double](sp.numCols)
            for (j <- 0 until sp.numCols) {
              arr(j) = sp.colPtrs(j + 1) - sp.colPtrs(j)
            }
            new DenseMatrix(1, sp.numCols, arr)
          } else { // CSR format
            val arr = new Array[Double](sp.numRows)
            for (i <- 0 until sp.rowIndices.length) {
              arr(sp.rowIndices(i)) += 1
            }
            new DenseMatrix(1, sp.numRows, arr)
          }
        case _ => throw new SparkException("Undefined matrix type in ColumnNnzDirectExecute()")
      }
      (cid, rowVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val cid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class ColumnNnzOnesExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val rowVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) {
            val arr = Array.fill[Double](den.numCols)(den.numRows)
            new DenseMatrix(1, den.numCols, arr)
          } else {
            val arr = Array.fill[Double](den.numRows)(den.numCols)
            new DenseMatrix(1, den.numRows, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) {
            val arr = Array.fill[Double](sp.numCols)(sp.numRows)
            new DenseMatrix(1, sp.numCols, arr)
          } else {
            val arr = Array.fill[Double](sp.numRows)(sp.numCols)
            new DenseMatrix(1, sp.numRows, arr)
          }
        case _ =>
          throw new SparkException("Undefined matrix type in ColumnNnzOnesExecution()")
      }
      (cid, rowVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val cid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class NnzDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val nnz = matrix match {
        case den: DenseMatrix =>
          new DenseMatrix(1, 1, Array[Double](den.values.count(x => math.abs(x) > 1e-6)))
        case sp: SparseMatrix =>
          new DenseMatrix(1, 1, Array[Double](sp.values.length))
        case _ =>
          throw new SparkException("Undefined matrix type in NnzDirectExecution()")
      }
      (0, nnz.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class NnzOnesExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val n = matrix match {
        case den: DenseMatrix =>
          new DenseMatrix(1, 1, Array[Double](den.numRows * den.numCols))
        case sp: SparseMatrix =>
          new DenseMatrix(1, 1, Array[Double](sp.numRows * sp.numCols))
        case _ => throw new SparkException("Undefined matrix type in NnzOnesExecution()")
      }
      (0, n.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.add(_, _))
    rdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class DiagNnzDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }
    val diagRdd = rdd.filter(tuple => tuple._1._1 == tuple._1._2).map { row =>
      val localDiag = row._2 match {
        case den: DenseMatrix =>
          val rowNum = den.numRows
          val colNum = den.numCols
          require(rowNum == colNum, s"block is not squre, row_num=$rowNum, col_num=$colNum")
          val values = den.values
          var nnz = 0
          for (j <- 0 until colNum) {
            if (math.abs(values(j * colNum + j)) > 1e-6) {
              nnz += 1
            }
          }
          val diagMat = new DenseMatrix(1, 1, Array[Double](nnz))
          (0, diagMat.asInstanceOf[MLMatrix])
        case sp: SparseMatrix =>
          var nnz = 0
          val values = sp.values
          val rowIndices = sp.rowIndices
          val colPtrs = sp.colPtrs
          for (j <- 0 until sp.numCols) {
            for (k <- 0 until colPtrs(j + 1) - colPtrs(j)) {
              if (j == rowIndices(k + colPtrs(j))) {
                nnz += 1
              }
            }
          }
          val diagMat = new DenseMatrix(1, 1, Array[Double](nnz))
          (0, diagMat.asInstanceOf[MLMatrix])
        case _ =>
          throw new SparkException("Undefined matrix type in DiagNnzDirectExecution()")
      }
      localDiag
    }.reduceByKey(LocalMatrix.add(_, _))

    diagRdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class DiagNnzOnesExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }
    val diagRdd = rdd.filter(tuple => tuple._1._1 == tuple._1._2).map { row =>
      val localDiag = row._2 match {
        case den: DenseMatrix =>
          require(den.numRows == den.numCols, s"block is not square")
          val mat = new DenseMatrix(1, 1, Array[Double](den.numRows))
          (0, mat.asInstanceOf[MLMatrix])
        case sp: SparseMatrix =>
          val mat = new DenseMatrix(1, 1, Array[Double](sp.numRows))
          (0, mat.asInstanceOf[MLMatrix])
        case _ =>
          throw new SparkException("Undefined matrix type in DiagNnzOnesExecution()")
      }
      localDiag
    }.reduceByKey(LocalMatrix.add(_, _))
    diagRdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class RowMaxDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val colVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) { // row max
            val m = den.numRows
            val arr = Array.fill[Double](m)(Double.MinValue)
            val values = den.values
            for (i <- 0 until values.length) {
              arr(i % m) = math.max(values(i), arr(i % m))
            }
            new DenseMatrix(m, 1, arr)
          } else { // column max
            val n = den.numCols
            val arr = Array.fill[Double](n)(Double.MinValue)
            val values = den.values
            for (i <- 0 until n) {
              for (j <- 0 until den.numRows) {
                arr(i) = math.max(arr(i), values(i * den.numRows + j))
              }
            }
            new DenseMatrix(n, 1, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            val arr = Array.fill[Double](sp.numRows)(Double.MinValue)
            for (i <- 0 until sp.rowIndices.length) {
              arr(sp.rowIndices(i)) = math.max(arr(sp.rowIndices(i)), sp.values(i))
            }
            new DenseMatrix(sp.numRows, 1, arr)
          } else { // CSR format
            val arr = Array.fill[Double](sp.numCols)(Double.MinValue)
            for (j <- 0 until sp.numCols) {
              for (i <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                arr(j) = math.max(arr(j), sp.values(i + sp.colPtrs(j)))
              }
            }
            new DenseMatrix(sp.numCols, 1, arr)
          }
        case _ => throw new SparkException("Undefined matrix type in RowMaxDirectExecution()")
      }
      (rid, colVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.max(_, _))
    rdd.map { blk =>
      val rid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class ColumnMaxDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val rowVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) {
            val n = den.numCols
            val arr = Array.fill[Double](n)(Double.MinValue)
            val values = den.values
            for (i <- 0 until n) {
              for (j <- 0 until den.numRows) {
                arr(i) = math.max(arr(i), values(i * den.numRows + j))
              }
            }
            new DenseMatrix(1, n, arr)
          } else {
            val m = den.numRows
            val arr = Array.fill[Double](m)(Double.MinValue)
            val values = den.values
            for (i <- 0 until values.length) {
              arr(i % m) = math.max(arr(i % m), values(i))
            }
            new DenseMatrix(1, m, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            val arr = Array.fill[Double](sp.numCols)(Double.MinValue)
            for (j <- 0 until sp.numCols) {
              for (i <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                arr(j) = math.max(arr(j), sp.values(i + sp.colPtrs(j)))
              }
            }
            new DenseMatrix(1, sp.numCols, arr)
          } else { // CSR format
            val arr = Array.fill[Double](sp.numRows)(Double.MinValue)
            for (i <- 0 until sp.rowIndices.length) {
              arr(sp.rowIndices(i)) = math.max(arr(sp.rowIndices(i)), sp.values(i))
            }
            new DenseMatrix(1, sp.numRows, arr)
          }
        case _ => throw new SparkException("Undefined matrix type in ColumnMaxDirectExecution()")
      }
      (cid, rowVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.max(_, _))
    rdd.map { blk =>
      val cid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class MaxDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val scalar = matrix match {
        case den: DenseMatrix =>
          new DenseMatrix(1, 1, Array[Double](den.values.max))
        case sp: SparseMatrix =>
          new DenseMatrix(1, 1, Array[Double](sp.values.max))
        case _ =>
          throw new SparkException("Undefined matrix type in MaxDirectExecution()")
      }
      (0, scalar.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.max(_, _))
    rdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class DiagMaxDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }
    val diagRdd = rdd.filter(tuple => tuple._1._1 == tuple._1._2).map { row =>
      val localDiag = row._2 match {
        case den: DenseMatrix =>
          val rowNum = den.numRows
          val colNum = den.numCols
          require(rowNum == colNum, s"block is not square, row_num=$rowNum, col_num=$colNum")
          val values = den.values
          var dmax = Double.MinValue
          for (j <- 0 until colNum) {
            dmax = math.max(dmax, values(j * colNum + j))
          }
          val mat = new DenseMatrix(1, 1, Array[Double](dmax))
          (0, mat.asInstanceOf[MLMatrix])
        case sp: SparseMatrix =>
          var dmax = Double.MinValue
          val values = sp.values
          val rowIndices = sp.rowIndices
          val colPtrs = sp.colPtrs
          for (j <- 0 until sp.numCols) {
            for (k <- 0 until colPtrs(j + 1) - colPtrs(j)) {
              if (rowIndices(k + colPtrs(j)) == j) {
                dmax = math.max(dmax, values(k + colPtrs(j)))
              }
            }
          }
          val mat = new DenseMatrix(1, 1, Array[Double](dmax))
          (0, mat.asInstanceOf[MLMatrix])
        case _ =>
          throw new SparkException("Undefined matrix type in DiagMaxDirectExecution()")
      }
      localDiag
    }.reduceByKey(LocalMatrix.max(_, _))

    diagRdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class RowMinDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val colVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) { // row min
            val m = den.numRows
            val arr = Array.fill[Double](m)(Double.MaxValue)
            val values = den.values
            for (i <- 0 until values.length) {
              arr(i % m) = math.min(values(i), arr(i % m))
            }
            new DenseMatrix(m, 1, arr)
          } else { // column min
            val n = den.numCols
            val arr = Array.fill[Double](n)(Double.MaxValue)
            val values = den.values
            for (i <- 0 until n) {
              for (j <- 0 until den.numRows) {
                arr(i) = math.min(arr(i), values(i * den.numRows + j))
              }
            }
            new DenseMatrix(n, 1, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            val arr = Array.fill[Double](sp.numRows)(Double.MaxValue)
            for (i <- 0 until sp.rowIndices.length) {
              arr(sp.rowIndices(i)) = math.min(arr(sp.rowIndices(i)), sp.values(i))
            }
            new DenseMatrix(sp.numRows, 1, arr)
          } else { // CSR format
            val arr = Array.fill[Double](sp.numCols)(Double.MaxValue)
            for (j <- 0 until sp.numCols) {
              for (i <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                arr(j) = math.min(arr(j), sp.values(i + sp.colPtrs(j)))
              }
            }
            new DenseMatrix(sp.numCols, 1, arr)
          }
        case _ => throw new SparkException("Undefined matrix type in RowMinDirectExecution()")
      }
      (rid, colVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.min(_, _))
    rdd.map { blk =>
      val rid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class ColumnMinDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val rowVec = matrix match {
        case den: DenseMatrix =>
          if (!den.isTransposed) {
            val n = den.numCols
            val arr = Array.fill[Double](n)(Double.MaxValue)
            val values = den.values
            for (i <- 0 until n) {
              for (j <- 0 until den.numRows) {
                arr(i) = math.min(arr(i), values(i * den.numRows + j))
              }
            }
            new DenseMatrix(1, n, arr)
          } else {
            val m = den.numRows
            val arr = Array.fill[Double](m)(Double.MaxValue)
            val values = den.values
            for (i <- 0 until values.length) {
              arr(i % m) = math.min(arr(i % m), values(i))
            }
            new DenseMatrix(1, m, arr)
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            val arr = Array.fill[Double](sp.numCols)(Double.MaxValue)
            for (j <- 0 until sp.numCols) {
              for (i <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                arr(j) = math.min(arr(j), sp.values(i + sp.colPtrs(j)))
              }
            }
            new DenseMatrix(1, sp.numCols, arr)
          } else { // CSR format
            val arr = Array.fill[Double](sp.numRows)(Double.MaxValue)
            for (i <- 0 until sp.rowIndices.length) {
              arr(sp.rowIndices(i)) = math.min(arr(sp.rowIndices(i)), sp.values(i))
            }
            new DenseMatrix(1, sp.numRows, arr)
          }
        case _ => throw new SparkException("Undefined matrix type in ColumnMinDirectExecution()")
      }
      (cid, rowVec.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.min(_, _))
    rdd.map { blk =>
      val cid = blk._1
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class MinDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val scalar = matrix match {
        case den: DenseMatrix =>
          new DenseMatrix(1, 1, Array[Double](den.values.min))
        case sp: SparseMatrix =>
          new DenseMatrix(1, 1, Array[Double](sp.values.min))
        case _ =>
          throw new SparkException("Undefined matrix type in MinDirectExecution()")
      }
      (0, scalar.asInstanceOf[MLMatrix])
    }.reduceByKey(LocalMatrix.min(_, _))
    rdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class DiagMinDirectExecution(child: SparkPlan) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    val rdd = rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }
    val diagRdd = rdd.filter(tuple => tuple._1._1 == tuple._1._2).map { row =>
      val localDiag = row._2 match {
        case den: DenseMatrix =>
          val rowNum = den.numRows
          val colNum = den.numCols
          require(rowNum == colNum, s"block is not square, row_num=$rowNum, col_num=$colNum")
          val values = den.values
          var dmin = Double.MaxValue
          for (j <- 0 until colNum) {
            dmin = math.min(dmin, values(j * colNum + j))
          }
          val mat = new DenseMatrix(1, 1, Array[Double](dmin))
          (0, mat.asInstanceOf[MLMatrix])
        case sp: SparseMatrix =>
          var dmin = Double.MaxValue
          val values = sp.values
          val rowIndices = sp.rowIndices
          val colPtrs = sp.colPtrs
          for (j <- 0 until sp.numCols) {
            for (k <- 0 until colPtrs(j + 1) - colPtrs(j)) {
              if (rowIndices(k + colPtrs(j)) == j) {
                dmin = math.min(dmin, values(k + colPtrs(j)))
              }
            }
          }
          val mat = new DenseMatrix(1, 1, Array[Double](dmin))
          (0, mat.asInstanceOf[MLMatrix])
        case _ =>
          throw new SparkException("Undefined matrix type in DiagMaxDirectExecution()")
      }
      localDiag
    }.reduceByKey(LocalMatrix.min(_, _))

    diagRdd.map { blk =>
      val res = new GenericInternalRow(3)
      res.setInt(0, 0)
      res.setInt(1, 0)
      res.update(2, MLMatrixSerializer.serialize(blk._2))
      res
    }
  }
}

case class MatrixScalarAddExecution(child: SparkPlan, alpha: Double) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      val res = new GenericInternalRow(3)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      val matrixRow = MLMatrixSerializer.serialize(LocalMatrix.addScalar(matrix, alpha))
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrixRow)
      res
    }
  }
}

case class MatrixScalarMultiplyExecution(child: SparkPlan, alpha: Double) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      val res = new GenericInternalRow(3)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      val matrixRow = MLMatrixSerializer.serialize(LocalMatrix.multiplyScalar(alpha, matrix))
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrixRow)
      res
    }
  }
}

case class MatrixPowerExecution(child: SparkPlan, alpha: Double) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    rootRdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      val res = new GenericInternalRow(3)
      val matrix = MLMatrixSerializer.deserialize(matrixInternalRow)
      val matrixRow = MLMatrixSerializer.serialize(LocalMatrix.matrixPow(matrix, alpha))
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrixRow)
      res
    }
  }
}

case class VectorizeExecution(child: SparkPlan,
                              nrows: Long, ncols: Long, blkSize: Int) extends MatfastPlan {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()
    val ROW_BLK_NUM = math.ceil(nrows * 1.0 / blkSize).toInt
    rdd.flatMap { row =>
      val i = row.getInt(0)
      val j = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val arr = matrix.toArray
      val numLocalRows = matrix.numRows
      val numLocalCols = matrix.numCols
      val buffer = ArrayBuffer[((Int, Int), MLMatrix)]()
      for (t <- 0 until numLocalCols) {
        val key = (j * ROW_BLK_NUM * blkSize + t * ROW_BLK_NUM + i, 0)
        val vecArray = new Array[Double](numLocalRows)
        for (k <- 0 until numLocalRows) {
          vecArray(k) = arr(t * numLocalCols + k)
        }
        buffer.append((key, new DenseMatrix(vecArray.length, 1, vecArray)))
      }
      buffer
    }.map { row =>
      val res = new GenericInternalRow(3)
      res.setInt(0, row._1._1)
      res.setInt(1, row._1._2)
      res.update(2, MLMatrixSerializer.serialize(row._2))
      res
    }
  }
}

case class MatrixElementAddExecution(left: SparkPlan,
                                     leftRowNum: Long,
                                     leftColNum: Long,
                                     right: SparkPlan,
                                     rightRowNum: Long,
                                     rightColNum: Long,
                                     blkSize: Int) extends MatfastPlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    require(leftRowNum == rightRowNum, s"Row number not match, " +
      s"leftRowNum = $leftRowNum, rightRowNum = $rightRowNum")
    require(leftColNum == rightColNum, s"Col number not match, " +
      s"leftColNum = $leftColNum, rightColNum = $rightColNum")
    val rdd1 = left.execute()
    val rdd2 = right.execute()
    if (rdd1.partitioner != None) {
      val part = rdd1.partitioner.get
      MatfastExecutionHelper.addWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    } else if (rdd2.partitioner != None) {
      val part = rdd2.partitioner.get
      MatfastExecutionHelper.addWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    } else {
      val params = MatfastExecutionHelper.genBlockCyclicPartitioner(leftRowNum, leftColNum, blkSize)
      val part = new BlockCyclicPartitioner(params._1, params._2, params._3, params._4)
      MatfastExecutionHelper.addWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    }
  }
}

case class MatrixElementMultiplyExecution(left: SparkPlan,
                                          leftRowNum: Long,
                                          leftColNum: Long,
                                          right: SparkPlan,
                                          rightRowNum: Long,
                                          rightColNum: Long,
                                          blkSize: Int) extends MatfastPlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    require(leftRowNum == rightRowNum, s"Row number not match, " +
      s"leftRowNum = $leftRowNum, rightRowNum = $rightRowNum")
    require(leftColNum == rightColNum, s"Col number not match, " +
      s"leftColNum = $leftColNum, rightColNum = $rightColNum")
    val rdd1 = left.execute()
    val rdd2 = right.execute()
    if (rdd1.partitioner != None) {
      val part = rdd1.partitioner.get
      MatfastExecutionHelper.multiplyWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    } else if (rdd2.partitioner != None) {
      val part = rdd2.partitioner.get
      MatfastExecutionHelper.multiplyWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    } else {
      val params = MatfastExecutionHelper.genBlockCyclicPartitioner(leftRowNum, leftColNum, blkSize)
      val part = new BlockCyclicPartitioner(params._1, params._2, params._3, params._4)
      MatfastExecutionHelper.multiplyWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    }
  }
}

case class MatrixElementDivideExecution(left: SparkPlan,
                                        leftRowNum: Long,
                                        leftColNum: Long,
                                        right: SparkPlan,
                                        rightRowNum: Long,
                                        rightColNum: Long,
                                        blkSize: Int) extends MatfastPlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    require(leftRowNum == rightRowNum, s"Row number not match, " +
      s"leftRowNum = $leftRowNum, rightRowNum = $rightRowNum")
    require(leftColNum == rightColNum, s"Col number not match, " +
      s"leftColNum = $leftColNum, rightColNum = $rightColNum")
    val rdd1 = left.execute()
    val rdd2 = right.execute()
    if (rdd1.partitioner != None) {
      val part = rdd1.partitioner.get
      MatfastExecutionHelper.divideWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    } else if (rdd2.partitioner != None) {
      val part = rdd2.partitioner.get
      MatfastExecutionHelper.divideWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    } else {
      val params = MatfastExecutionHelper.genBlockCyclicPartitioner(leftRowNum, leftColNum, blkSize)
      val part = new BlockCyclicPartitioner(params._1, params._2, params._3, params._4)
      MatfastExecutionHelper.divideWithPartitioner(
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
        MatfastExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
    }
  }
}

case class MatrixMatrixMultiplicationExecution(left: SparkPlan,
                                               leftRowNum: Long,
                                               leftColNum: Long,
                                               right: SparkPlan,
                                               rightRowNum: Long,
                                               rightColNum: Long,
                                               blkSize: Int) extends MatfastPlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    // check for multiplication possibility
    require(leftColNum == rightRowNum, s"Matrix dimension not match, " +
      s"leftColNum = $leftColNum, rightRowNum = $rightRowNum")
    // estimate memory usage
    val memoryUsage = leftRowNum * rightColNum * 8 / (1024 * 1024 * 1024) * 1.0
    if (memoryUsage > 10) {
      // scalastyle:off
      println(s"Caution: matrix multiplication result size = $memoryUsage GB")
      // scalastyle:on
    }
    // compute number of row/col blocks for invoking special matrix multiplication procedure
    val leftColBlkNum = math.ceil(leftColNum * 1.0 / blkSize).toInt
    val rightRowBlkNum = math.ceil(rightRowNum * 1.0 / blkSize).toInt
    if (leftColBlkNum == 1 && rightRowBlkNum == 1) {
      val leftRowBlkNum = leftRowNum / blkSize
      val rightColBlkNum = rightColNum / blkSize
      if (leftRowBlkNum <= rightColBlkNum) {
        MatfastExecutionHelper.multiplyOuterProductDuplicateLeft(left.execute(), right.execute())
      } else {
        MatfastExecutionHelper.multiplyOuterProductDuplicateRight(left.execute(), right.execute())
      }
    } else {
      MatfastExecutionHelper.matrixMultiplyGeneral(left.execute(), right.execute())
    }
  }
}

case class RankOneUpdateExecution(left: SparkPlan,
                                  leftRowNum: Long,
                                  leftColNum: Long,
                                  right: SparkPlan,
                                  rightRowNum: Long,
                                  rightColNum: Long,
                                  blkSize: Int) extends MatfastPlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    require(rightRowNum == 1, s"Vector column size is not 1, but #cols = $rightRowNum")
    require(leftRowNum == rightRowNum, s"Dimension not match for matrix addition, " +
      s"A.nrows = $leftRowNum, " +
    s"A.ncols = ${leftColNum}, B.nrows = $rightRowNum, B.ncols = $rightColNum")
    MatfastExecutionHelper.matrixRankOneUpdate(left.execute(), right.execute())
  }
}

case class JoinTwoIndicesExecution(left: SparkPlan,
                                   leftRowNum: Long,
                                   leftColNum: Long,
                                   right: SparkPlan,
                                   rightRowNum: Long,
                                   rightColNum: Long,
                                   mergeFunc: (Double, Double) => Double,
                                   blkSize: Int) extends MatfastPlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    var rdd1 = left.execute().map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }
    var rdd2 = right.execute().map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }
    val p1 = rdd1.partitioner.getOrElse(null)
    val p2 = rdd2.partitioner.getOrElse(null)
    if (p1 == null && p2 == null) { // if no partitioner is applied, just use RowPartitioner
      val p = new RowPartitioner(32)
      rdd1 = rdd1.partitionBy(p)
      rdd2 = rdd2.partitionBy(p)
    } else {
      if (leftRowNum * leftColNum <= rightRowNum * rightColNum) { // left-hand side is smaller
        if (p2 != null) {
          rdd1 = rdd1.partitionBy(p2)
        } else {
          rdd2 = rdd2.partitionBy(p1)
        }
      } else {
        if (p1 != null) {
          rdd2 = rdd2.partitionBy(p1)
        } else {
          rdd1 = rdd1.partitionBy(p2)
        }
      }
    }
    val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) {
      case (iter1, iter2) =>
        val key2val = new TrieMap[(Int, Int), MLMatrix]()
        val res = new TrieMap[(Int, Int), MLMatrix]()
        for (elem <- iter1) {
          val key = elem._1
          if (!key2val.contains(key)) key2val.putIfAbsent(key, elem._2)
        }

        for (elem <- iter2) {
          val key = elem._1
          if (key2val.contains(key)) {
            val tmp = key2val.get(key).get
            res.putIfAbsent(key, LocalMatrix.compute(tmp, elem._2, mergeFunc))
          }
        }
        res.iterator
    }
    rdd.map { elem =>
      val rid = elem._1._1
      val cid = elem._1._2
      val matrix = elem._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(matrix))
      res
    }
  }
}

case class CrossProductExecution(left: SparkPlan,
                                 leftRowNum: Long,
                                 leftColNum: Long,
                                 right: SparkPlan,
                                 rightRowNum: Long,
                                 rightColNum: Long,
                                 mergeFunc: (Double, Double) => Double,
                                 blkSize: Int) extends MatfastPlan {

  lazy val dim: Seq[Attribute] =
    Seq(AttributeReference("dim1", LongType, nullable = false)(ExprId(1L)),
      AttributeReference("dim2", LongType, nullable = false)(ExprId(2L)))

  override def output: Seq[Attribute] = dim ++ right.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    val rdd1 = left.execute().map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }
    val rdd2 = right.execute().map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }
    val crossProduct = rdd1.cartesian(rdd2).flatMap { case (((rid1, cid1), mat1), ((rid2, cid2), mat2)) =>
      // (d1, d2, b3, b4) is different than the original blk ids
      // here (d1, d2) are the first two indices of the 4d blk
      // while (b3, b4) are the blk ids
        val offsetD1: Long = rid1 * blkSize
        val offsetD2: Long = cid1 * blkSize
        val joinEachFromMat1ToMat2 = new ArrayBuffer[((Long, Long, Int, Int), MLMatrix)]()
        for (i <- 0 until mat1.numRows) {
          for (j <- 0 until mat1.numCols) {
            val offsetRid = offsetD1 + i
            val offsetCid = offsetD2 + j
            val slice = ((offsetRid, offsetCid, rid2, cid2),
              LocalMatrix.UDF_Element_Matrix(mat1(i, j), mat2, mergeFunc))
            joinEachFromMat1ToMat2 += slice
          }
        }
        joinEachFromMat1ToMat2.iterator
    }
    crossProduct.map { elem =>
      val d1 = elem._1._1
      val d2 = elem._1._2
      val b3 = elem._1._3
      val b4 = elem._1._4
      val matrix = elem._2
      val res = new GenericInternalRow(5)
      res.setLong(0, d1)
      res.setLong(1, d2)
      res.setInt(2, b3)
      res.setInt(3, b4)
      res.update(4, MLMatrixSerializer.serialize(matrix))
      res
    }
  }
}

case class JoinOnValuesExecution(left: SparkPlan,
                                 leftRowNum: Long,
                                 leftColNum: Long,
                                 right: SparkPlan,
                                 rightRowNum: Long,
                                 rightColNum: Long,
                                 mergeFunc: (Double, Double) => Double,
                                 blkSize: Int) extends MatfastPlan {

  lazy val dim: Seq[Attribute] =
    Seq(AttributeReference("dim1", LongType, nullable = false)(ExprId(1L)),
      AttributeReference("dim2", LongType, nullable = false)(ExprId(2L)))

  override def output: Seq[Attribute] = dim ++ right.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    val rdd1 = left.execute().map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }

    val rdd2 = right.execute().map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      ((rid, cid), matrix)
    }

    val joinValueRdd = rdd1.cartesian(rdd2).flatMap { case (((rid1, cid1), mat1), ((rid2, cid2), mat2)) =>
        val offsetD1: Long = rid1 * blkSize
        val offsetD2: Long = cid1 * blkSize
        val joinRes = new ArrayBuffer[((Long, Long, Int, Int), MLMatrix)]()
        for (i <- 0 until mat1.numRows) {
          for (j <- 0 until mat1.numCols) {
            val offsetRid = offsetD1 + i
            val offsetCid = offsetD2 + j
            val join = LocalMatrix.UDF_Element_Match(mat1(i, j), mat2, mergeFunc)
            if (join._1) {
              val insert = ((offsetRid, offsetCid, rid2, cid2), join._2)
              joinRes += insert
            }
          }
        }
        joinRes.iterator
    }
    joinValueRdd.map { elem =>
      val d1 = elem._1._1
      val d2 = elem._1._2
      val b3 = elem._1._3
      val b4 = elem._1._4
      val matrix = elem._2
      val res = new GenericInternalRow(5)
      res.setLong(0, d1)
      res.setLong(1, d2)
      res.setInt(2, b3)
      res.setInt(3, b4)
      res.update(4, MLMatrixSerializer.serialize(matrix))
      res
    }
  }
}