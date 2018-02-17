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

import scala.collection.concurrent.TrieMap
import scala.util.control.Breaks._
import org.apache.spark.{Partitioner, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.matfast.matrix.{MLMatrix, _}
import org.apache.spark.sql.matfast.partitioner.{BlockCyclicPartitioner, ColumnPartitioner, IndexPartitioner, RowPartitioner}
import org.apache.spark.sql.matfast.util.MLMatrixSerializer

import scala.collection.mutable.ArrayBuffer

object MatfastExecutionHelper {

  def repartitionWithTargetPartitioner(partitioner: Partitioner,
    rdd: RDD[InternalRow]): RDD[((Int, Int), InternalRow)] = {
    partitioner match {
      case rowPart: RowPartitioner => RowPartitioner(rdd, rowPart.numPartitions)
      case colPart: ColumnPartitioner => ColumnPartitioner(rdd, colPart.numPartitions)
      case cyclicPart: BlockCyclicPartitioner =>
        BlockCyclicPartitioner(rdd, cyclicPart.ROW_BLKS, cyclicPart.COL_BLKS,
        cyclicPart.ROW_BLKS_PER_PARTITION, cyclicPart.COL_BLKS_PER_PARTITION)
      case _ => throw new IllegalArgumentException(s"Partitioner not recognized for $partitioner")
    }
  }

  def genBlockCyclicPartitioner(nrows: Long, ncols: Long, blkSize: Int): (Int, Int, Int, Int) = {
    val ROW_BLK_NUM = math.ceil(nrows * 1.0 / blkSize).toInt
    val COL_BLK_NUM = math.ceil(ncols * 1.0 / blkSize).toInt
    val numPartitions = 64
    val scale = 1.0 / math.sqrt(numPartitions)
    var ROW_BLKS_PER_PARTITION = math.round(math.max(scale * ROW_BLK_NUM, 1.0)).toInt
    var COL_BLKS_PER_PARTITION = math.round(math.max(scale * COL_BLK_NUM, 1.0)).toInt
    if (ROW_BLKS_PER_PARTITION == 1 || COL_BLKS_PER_PARTITION == 1) {
      if (ROW_BLKS_PER_PARTITION != 1) {
        ROW_BLKS_PER_PARTITION = math.round(math.max(ROW_BLKS_PER_PARTITION / 8.0, 1.0)).toInt
      }
      if (COL_BLKS_PER_PARTITION != 1) {
        COL_BLKS_PER_PARTITION = math.round(math.max(COL_BLKS_PER_PARTITION / 8.0, 1.0)).toInt
      }
    }
    (ROW_BLK_NUM, COL_BLK_NUM, ROW_BLKS_PER_PARTITION, COL_BLKS_PER_PARTITION)
  }

  def addWithPartitioner(rdd1: RDD[((Int, Int), InternalRow)],
                         rdd2: RDD[((Int, Int), InternalRow)]): RDD[InternalRow] = {

    val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { (iter1, iter2) =>
      val buf = new TrieMap[(Int, Int), InternalRow]()
      for (a <- iter1) {
        if (a != null) {
          val idx = a._1
          if (!buf.contains(idx)) buf.putIfAbsent(idx, a._2)
          else {
            val old = buf.get(idx).get
            val res = LocalMatrix.add(MLMatrixSerializer.deserialize(old),
              MLMatrixSerializer.deserialize(a._2))
            buf.put(idx, MLMatrixSerializer.serialize(res))
          }
        }
      }
      for (b <- iter2) {
        if (b != null) {
          val idx = b._1
          if (!buf.contains(idx)) buf.putIfAbsent(idx, b._2)
          else {
            val old = buf.get(idx).get
            val res = LocalMatrix.add(MLMatrixSerializer.deserialize(old),
              MLMatrixSerializer.deserialize(b._2))
            buf.put(idx, MLMatrixSerializer.serialize(res))
          }
        }
      }
      buf.iterator
    }
    rdd.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrix)
      res
    }
  }

  def multiplyWithPartitioner(rdd1: RDD[((Int, Int), InternalRow)],
                         rdd2: RDD[((Int, Int), InternalRow)]): RDD[InternalRow] = {

    val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { case (iter1, iter2) =>
        val idx2val = new TrieMap[(Int, Int), InternalRow]()
        val res = new TrieMap[(Int, Int), InternalRow]()
        for (elem <- iter1) {
          val key = elem._1
          if (!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2)
        }
        for (elem <- iter2) {
          val key = elem._1
          if (idx2val.contains(key)) {
            val tmp = idx2val.get(key).get
            val product = MLMatrixSerializer.serialize(
              LocalMatrix.elementWiseMultiply(MLMatrixSerializer.deserialize(tmp),
              MLMatrixSerializer.deserialize(elem._2)))
            res.putIfAbsent(key, product)
          }
        }
        res.iterator
    }
    rdd.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrix)
      res
    }
  }

  def divideWithPartitioner(rdd1: RDD[((Int, Int), InternalRow)],
                            rdd2: RDD[((Int, Int), InternalRow)]): RDD[InternalRow] = {

    val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { case (iter1, iter2) =>
      val idx2val = new TrieMap[(Int, Int), InternalRow]()
      val res = new TrieMap[(Int, Int), InternalRow]()
      for (elem <- iter1) {
        val key = elem._1
        if (!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2)
      }
      for (elem <- iter2) {
        val key = elem._1
        if (idx2val.contains(key)) {
          val tmp = idx2val.get(key).get
          val division = MLMatrixSerializer.serialize(
            LocalMatrix.elementWiseDivide(MLMatrixSerializer.deserialize(tmp),
            MLMatrixSerializer.deserialize(elem._2)))
          res.putIfAbsent(key, division)
        }
      }
      res.iterator
    }
    rdd.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrix)
      res
    }
  }

  def multiplyOuterProductDuplicateLeft(rdd1: RDD[InternalRow],
                                        rdd2: RDD[InternalRow]): RDD[InternalRow] = {
    val numPartitions = rdd2.partitions.length
    val rightRDD = repartitionWithTargetPartitioner(new ColumnPartitioner(numPartitions), rdd2)
    val dupRDD = duplicateCrossPartitions(rdd1, numPartitions)
    dupRDD.zipPartitions(rightRDD, preservesPartitioning = true) { (iter1, iter2) =>
      val dup = iter1.next()._2
      for {
        x2 <- iter2
        x1 <- dup
      } yield ((x1.rid, x2._1._2),
        LocalMatrix.matrixMultiplication(x1.matrix, MLMatrixSerializer.deserialize(x2._2)))
    }.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(3, MLMatrixSerializer.serialize(matrix))
      res
    }
  }

  def multiplyOuterProductDuplicateRight(rdd1: RDD[InternalRow],
                                         rdd2: RDD[InternalRow]): RDD[InternalRow] = {
    val numPartitions = rdd1.partitions.length
    val leftRDD = repartitionWithTargetPartitioner(new RowPartitioner(numPartitions), rdd1)
    val dupRDD = duplicateCrossPartitions(rdd2, numPartitions)
    leftRDD.zipPartitions(dupRDD, preservesPartitioning = true) { (iter1, iter2) =>
      val dup = iter2.next()._2
      for {
        x1 <- iter1
        x2 <- dup
      } yield ((x1._1._1, x2.cid),
        LocalMatrix.matrixMultiplication(MLMatrixSerializer.deserialize(x1._2), x2.matrix))
    }.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(matrix))
      res
    }
  }

  // duplicate the matrix for #partitions copies and distribute them over the cluster
  private def duplicateCrossPartitions(rdd: RDD[InternalRow],
                                       numPartitions: Int): RDD[(Int, Iterable[MatrixBlock])] = {
    rdd.flatMap { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      for (i <- 0 until numPartitions)
        yield (i, MatrixBlock(rid, cid, matrix))
    }.groupByKey(new IndexPartitioner(numPartitions))
  }

  def matrixMultiplyGeneral(rdd1: RDD[InternalRow], rdd2: RDD[InternalRow]): RDD[InternalRow] = {
    val leftRdd = rdd1.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      (cid, (rid, matrixInternalRow))
    }.groupByKey()
    val rightRdd = rdd2.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      (rid, (cid, matrixInternalRow))
    }.groupByKey()
    leftRdd.join(rightRdd)
           .values
           .flatMap { case (iter1, iter2) =>
               for (blk1 <- iter1; blk2 <- iter2)
                 yield ((blk1._1, blk2._1),
                   LocalMatrix.matrixMultiplication(MLMatrixSerializer.deserialize(blk1._2),
                     MLMatrixSerializer.deserialize(blk2._2)))
           }.reduceByKey(LocalMatrix.add(_, _))
            .map { row =>
              val res = new GenericInternalRow(3)
              res.setInt(0, row._1._1)
              res.setInt(1, row._1._2)
              res.update(2, MLMatrixSerializer.serialize(row._2))
              res
            }
  }

  def matrixRankOneUpdate(rdd1: RDD[InternalRow], rdd2: RDD[InternalRow]): RDD[InternalRow] = {
    val numPartitions = rdd1.partitions.length
    val dupRdd = duplicateCrossPartitions(rdd2, numPartitions)
    rdd1.zipPartitions(dupRdd, preservesPartitioning = true) { (iter1, iter2) =>
      val dup = iter2.next()._2
      for {
        x1 <- iter1
        x2 <- dup
        x3 <- dup
        if (x1.getInt(0) == x2.rid && x1.getInt(1) == x3.rid)
      } yield (x1.getInt(0), x1.getInt(1),
        LocalMatrix.rankOneAdd(MLMatrixSerializer.deserialize(
          x1.getStruct(2, 7)), x2.matrix, x3.matrix))
    }.map { row =>
      val res = new GenericInternalRow(3)
      res.setInt(0, row._1)
      res.setInt(1, row._2)
      res.update(2, MLMatrixSerializer.serialize(row._3))
      res
    }
  }

  def findEmptyRows(rdd: RDD[InternalRow]): RDD[(Int, Set[Int])] = {
    rdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val tuple = matrix match {
        case den: DenseMatrix =>
          var rowIndexInclude = scala.collection.mutable.Set[Int]()
          breakable {
            for (i <- 0 until den.numRows) {
              if (!rowIndexInclude.contains(i)) {
                for (j <- 0 until den.numCols) {
                  if (den(i, j) != 0.0) {
                    rowIndexInclude += i
                    break
                  }
                }
              }
            }
          }
          var rowIndexNotExist = scala.collection.mutable.Set[Int]()
          for (i <- 0 until den.numRows) {
            if (!rowIndexInclude.contains(i)) {
              rowIndexNotExist += i
            }
          }
          rowIndexNotExist
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC
            var rowIndexInclude = scala.collection.mutable.Set[Int]()
            for (x <- sp.rowIndices) {
              if (!rowIndexInclude.contains(x)) {
                rowIndexInclude += x
              }
            }
            var rowIndexNotExist = scala.collection.mutable.Set[Int]()
            for (i <- 0 until sp.numRows) {
              if (!rowIndexInclude.contains(i)) {
                rowIndexNotExist += i
              }
            }
            rowIndexNotExist
          } else { // CSR
            var rowIndexNotExist = scala.collection.mutable.Set[Int]()
            for (i <- 0 until sp.numCols) {
              if (sp.colPtrs(i + 1) == sp.colPtrs(i)) {
                rowIndexNotExist += i
              }
            }
            rowIndexNotExist
          }
        case _ =>
          throw new SparkException("Undefined matrix type in findEmptyRows()")
      }
      (rid, (cid, tuple))
    }.groupByKey()
      .map { case (rid, iter) =>
          val rowMap = scala.collection.mutable.HashMap[Int, Int]()
          var ncols = 0
          for (tuple <- iter) {
            val currSet = tuple._2
            ncols += 1
            for (x <- currSet) {
              if (!rowMap.contains(x)) {
                rowMap(x) = 1
              } else {
                rowMap(x) = rowMap(x) + 1
              }
            }
          }
          var rowsNotExist = scala.collection.mutable.Set[Int]()
          for (key <- rowMap.keySet) {
            if (ncols == rowMap(key)) {
              rowsNotExist += key
            }
          }
        (rid, rowsNotExist.toSet)
      }
  }

  def removeEmptyRows(rdd: RDD[InternalRow],
                      removedRows: RDD[(Int, Set[Int])]): RDD[InternalRow] = {
    val matRdd = rdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      (rid, (cid, matrix))
    }
    matRdd.join(removedRows).map { case (rid, ((cid, matrix), rowSet)) =>
    val newMatrix = matrix match {
        case den: DenseMatrix =>
          if (rowSet.isEmpty) {
            den
          } else {
            val newLength = den.numCols * (den.numRows - rowSet.size)
            val newValues = Array.fill(newLength)(0.0)
            var i = 0
            for (x <- den.values) {
              if (x != 0.0) {
                newValues(i) = x
                i += 1
              }
            }
            if (!den.isTransposed) {
              new DenseMatrix(den.numRows - rowSet.size, den.numCols, newValues, den.isTransposed)
            } else {
              new DenseMatrix(den.numRows, den.numCols - rowSet.size, newValues, den.isTransposed)
            }
          }
        case sp: SparseMatrix =>
          if (rowSet.isEmpty) {
            sp
          } else {
            if (!sp.isTransposed) {
              val rowIdx = sp.rowIndices
              for (i <- 0 until rowIdx.length) {
                var smaller = 0
                for (rmIdx <- rowSet) {
                  if (rmIdx < rowIdx(i)) {
                    smaller += 1
                  }
                }
                rowIdx(i) -= smaller
              }
              new SparseMatrix(sp.numRows - rowSet.size, sp.numCols,
                sp.colPtrs, rowIdx, sp.values, sp.isTransposed)
            } else {
              val rowIdx = Array.fill(sp.numCols - rowSet.size + 1)(0)
              var ind = 0
              for (i <- 0 until sp.colPtrs.length - 1) {
                if (sp.colPtrs(i + 1) != sp.colPtrs(i)) {
                  rowIdx(ind) = sp.colPtrs(i)
                  ind += 1
                }
              }
              rowIdx(rowIdx.length - 1) = sp.colPtrs(sp.colPtrs.length - 1)
              new SparseMatrix(sp.numCols - rowSet.size, sp.numRows, rowIdx,
                sp.rowIndices, sp.values, sp.isTransposed)
            }
          }
        case _ =>
          throw new SparkException("Undefined matrix type in RemoveEmptyRowsDirectExecute()")
      }
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(newMatrix.asInstanceOf[MLMatrix]))
      res
    }
  }

  def findEmptyColumns(rdd: RDD[InternalRow]): RDD[(Int, Set[Int])] = {
    rdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val tuple = matrix match {
        case den: DenseMatrix =>
          var columnIndexInclude = scala.collection.mutable.Set[Int]()
          breakable {
            for (j <- 0 until den.numCols) {
              if (!columnIndexInclude.contains(j)) {
                for (i <- 0 until den.numRows) {
                  if (den(i, j) != 0.0) {
                    columnIndexInclude += j
                    break
                  }
                }
              }
            }
          }
          var columnIndexNotExist = scala.collection.mutable.Set[Int]()
          for (j <- 0 until den.numCols) {
            if (!columnIndexInclude.contains(j)) {
              columnIndexNotExist += j
            }
          }
          columnIndexNotExist
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC
            var columnIndexNotExist = scala.collection.mutable.Set[Int]()
            for (i <- 0 until sp.numCols) {
              if (sp.colPtrs(i + 1) == sp.colPtrs(i)) {
                columnIndexNotExist += i
              }
            }
            columnIndexNotExist
          } else { // CSR
            var columnIndexInclude = scala.collection.mutable.Set[Int]()
            for (x <- sp.rowIndices) {
              if (!columnIndexInclude.contains(x)) {
                columnIndexInclude += x
              }
            }
            var columnIndexNotExist = scala.collection.mutable.Set[Int]()
            for (j <- 0 until sp.numCols) {
              if (!columnIndexInclude.contains(j)) {
                columnIndexNotExist += j
              }
            }
            columnIndexNotExist
          }
        case _ =>
          throw new SparkException("Undefined matrix type in FindEmptyColumns()")
      }
      (cid, (rid, tuple))
    }.groupByKey()
      .map { case (cid, iter) =>
          val colMap = scala.collection.mutable.HashMap[Int, Int]()
          var nrows = 0
          for (tuple <- iter) {
            val currSet = tuple._2
            nrows += 1
            for (x <- currSet) {
              if (!colMap.contains(x)) {
                colMap(x) = 1
              } else {
                colMap(x) = colMap(x) + 1
              }
            }
          }
          var colsNotExist = scala.collection.mutable.Set[Int]()
          for (key <- colMap.keySet) {
            if (nrows == colMap(key)) {
              colsNotExist += key
            }
          }
        (cid, colsNotExist.toSet)
      }
  }

  def removeEmptyColumns(rdd: RDD[InternalRow],
                         removedColumns: RDD[(Int, Set[Int])]): RDD[InternalRow] = {
    val matRdd = rdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      (cid, (rid, matrix))
    }
    matRdd.join(removedColumns).map { case (cid, ((rid, matrix), colSet)) =>
      val newMatrix = matrix match {
        case den: DenseMatrix =>
          if (colSet.isEmpty) {
            den
          } else {
            val newLength = den.numRows * (den.numCols - colSet.size)
            val newValues = Array.fill(newLength)(0.0)
            var i = 0
            for (x <- den.values) {
              if (x != 0.0) {
                newValues(i) = x
                i += 1
              }
            }
            if (!den.isTransposed) {
              new DenseMatrix(den.numRows, den.numCols - colSet.size, newValues, den.isTransposed)
            } else {
              new DenseMatrix(den.numRows - colSet.size, den.numCols, newValues, den.isTransposed)
            }
          }
        case sp: SparseMatrix =>
          if (colSet.isEmpty) {
            sp
          } else {
            if (!sp.isTransposed) {
              val colIdx = Array.fill(sp.numCols - colSet.size + 1)(0)
              var ind = 0
              for (i <- 0 until sp.colPtrs.length - 1) {
                if (sp.colPtrs(i + 1) != sp.colPtrs(i)) {
                  colIdx(ind) = sp.colPtrs(i)
                  ind += 1
                }
              }
              colIdx(colIdx.length - 1) = sp.colPtrs(sp.colPtrs.length - 1)
              new SparseMatrix(sp.numRows, sp.numCols - colSet.size, colIdx,
                sp.rowIndices, sp.values, sp.isTransposed)
            } else {
              val colIdx = sp.rowIndices
              for (i <- 0 until colIdx.length) {
                var smaller = 0
                for (rmIdx <- colSet) {
                  if (rmIdx < colIdx(i)) {
                    smaller += 1
                  }
                }
                colIdx(i) -= smaller
              }
              new SparseMatrix(sp.numCols, sp.numRows - colSet.size, sp.colPtrs,
                colIdx, sp.values, sp.isTransposed)
            }
          }
        case _ =>
          throw new SparkException("Undefined matrix type in RemoveEmptyColumnsDirectExecute()")
      }
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(newMatrix.asInstanceOf[MLMatrix]))
      res
    }
  }

  def selfElementMultiply(rdd: RDD[InternalRow]): RDD[InternalRow] = {
    rdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val mat = matrix match {
        case den: DenseMatrix =>
          val arr = den.values.map(x => x*x)
          new DenseMatrix(den.numRows, den.numCols, arr, den.isTransposed)
        case sp: SparseMatrix =>
          val arr = sp.values.map(x => x*x)
          new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, arr, sp.isTransposed)
        case _ => throw new SparkException("Not supported matrix type")
      }
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(mat))
      res
    }
  }

  def selfElementAdd(rdd: RDD[InternalRow]): RDD[InternalRow] = {
    rdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val mat = matrix match {
        case den: DenseMatrix =>
          val arr = den.values.map(x => 2*x)
          new DenseMatrix(den.numRows, den.numCols, arr, den.isTransposed)
        case sp: SparseMatrix =>
          val arr = sp.values.map(x => x*x)
          new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, arr, sp.isTransposed)
        case _ => throw new SparkException("Not supported matrix type")
      }
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(mat))
      res
    }
  }

  def selfElementDivide(rdd: RDD[InternalRow]): RDD[InternalRow] = {
    rdd.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      val mat = matrix match {
        case den: DenseMatrix =>
          val arr = den.values.map(_ => 1.0)
          new DenseMatrix(den.numRows, den.numCols, arr, den.isTransposed)
        case sp: SparseMatrix =>
          val arr = sp.values.map(x => x*x)
          new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, arr, sp.isTransposed)
        case _ => throw new SparkException("Not supported matrix type")
      }
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(mat))
      res
    }
  }

  def joinOnValuesDuplicateLeft(rdd1: RDD[InternalRow],
                                rdd2: RDD[InternalRow],
                                udf: (Double, Double) => Double,
                                blkSize: Int): RDD[InternalRow] = {

    val numPartitions = rdd2.partitions.length
    val rightRdd = repartitionWithTargetPartitioner(new ColumnPartitioner(numPartitions), rdd2)
    val dupRdd = duplicateCrossPartitions(rdd1, numPartitions)
    dupRdd.zipPartitions(rightRdd, preservesPartitioning = true) { (iter1, iter2) =>
      val dup = iter1.next()._2
      for {
        x2 <- iter2
        x1 <- dup
      } yield (computeJoinValues(x1.rid, x1.cid, x1.matrix,
        x2._1._1, x2._1._2, MLMatrixSerializer.deserialize(x2._2), udf, blkSize))
    }.flatMap(_.iterator)
      .map { row =>
        val d1 = row._1._1
        val d2 = row._1._2
        val d3 = row._1._3
        val d4 = row._1._4
        val matrix = row._2
        val res = new GenericInternalRow(5)
        res.setInt(0, d1)
        res.setInt(1, d2)
        res.setInt(2, d3)
        res.setInt(3, d4)
        res.update(4, MLMatrixSerializer.serialize(matrix))
        res
      }
  }

  def joinOnValuesDuplicateRight(rdd1: RDD[InternalRow],
                                 rdd2: RDD[InternalRow],
                                 udf: (Double, Double) => Double,
                                 blkSize: Int): RDD[InternalRow] = {

    val numPartitions = rdd1.partitions.length
    val leftRdd = repartitionWithTargetPartitioner(new RowPartitioner(numPartitions), rdd1)
    val dupRdd = duplicateCrossPartitions(rdd2, numPartitions)
    leftRdd.zipPartitions(dupRdd, preservesPartitioning = true) { (iter1, iter2) =>
      val dup = iter2.next()._2
      for {
        x1 <- iter1
        x2 <- dup
      } yield (computeJoinValues(x1._1._1, x1._1._2, MLMatrixSerializer.deserialize(x1._2),
        x2.rid, x2.cid, x2.matrix, udf, blkSize))
    }.flatMap(_.iterator)
      .map { row =>
        val d1 = row._1._1
        val d2 = row._1._2
        val d3 = row._1._3
        val d4 = row._1._4
        val matrix = row._2
        val res = new GenericInternalRow(5)
        res.setInt(0, d1)
        res.setInt(1, d2)
        res.setInt(2, d3)
        res.setInt(3, d4)
        res.update(4, MLMatrixSerializer.serialize(matrix))
        res
      }
  }


  def computeJoinValues(rid1: Int, cid1: Int, mat1: MLMatrix,
                        rid2: Int, cid2: Int, mat2: MLMatrix,
                        udf: (Double, Double) => Double,
                        blkSize: Int): ArrayBuffer[((Int, Int, Int, Int), MLMatrix)] = {

    val offsetD1: Int = rid1 * blkSize
    val offsetD2: Int = cid1 * blkSize
    val joinRes = new ArrayBuffer[((Int, Int, Int, Int), MLMatrix)]()
    val rand = new scala.util.Random()
    if (math.abs(udf(0.0, rand.nextDouble()) - 0.0) < 1e-6) {
      mat1 match {
        case den: DenseMatrix =>
          for (i <- 0 until den.numRows) {
            for (j <- 0 until den.numCols) {
              if (math.abs(den(i, j) - 0.0) > 1e-6) {
                val offsetRid = offsetD1 + i
                val offsetCid = offsetD2 + j
                val join = LocalMatrix.UDF_Cell_Match(den(i, j), mat2, udf)
                if (join._1) {
                  val insert = ((offsetRid, offsetCid, rid2, cid2), join._2)
                  joinRes += insert
                }
              }
            }
          }
        case sp: SparseMatrix =>
          if (!sp.isTransposed) { // CSC format
            for (j <- 0 until sp.numCols) {
              for (k <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                val ind = sp.colPtrs(j) + k
                val i = sp.rowIndices(ind)
                val offsetRid = offsetD1 + i
                val offsetCid = offsetD2 + j
                val join = LocalMatrix.UDF_Cell_Match(sp.values(ind), mat2, udf)
                if (join._1) {
                  val insert = ((offsetRid, offsetCid, rid2, cid2), join._2)
                  joinRes += insert
                }
              }
            }
          } else { // CSR format
            for (i <- 0 until sp.numRows) {
              for (k <- 0 until sp.colPtrs(i + 1) - sp.colPtrs(i)) {
               val ind = sp.colPtrs(i) + k
                val j = sp.rowIndices(ind)
                val offsetRid = offsetD1 + i
                val offsetCid = offsetD2 + j
                val join = LocalMatrix.UDF_Cell_Match(sp.values(ind), mat2, udf)
                if (join._1) {
                  val insert = ((offsetRid, offsetCid, rid2, cid2), join._2)
                  joinRes += insert
                }
              }
            }
          }
        case _ => throw new SparkException("Illegal matrix type")
      }
    } else {
      for (i <- 0 until mat1.numRows) {
        for (j <- 0 until mat1.numCols) {
          val offsetRid = offsetD1 + i
          val offsetCid = offsetD2 + j
          val join = LocalMatrix.UDF_Cell_Match(mat1(i, j), mat2, udf)
          if (join._1) {
            val insert = ((offsetRid, offsetCid, rid2, cid2), join._2)
            joinRes += insert
          }
        }
      }
    }
    joinRes
  }
}
