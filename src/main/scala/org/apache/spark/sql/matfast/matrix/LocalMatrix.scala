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

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}

import org.apache.spark.mllib.linalg.{DenseMatrix => SparkDense, Matrix => SparkMatrix, SparseMatrix => SparkSparse}
import org.apache.spark.SparkException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._

/**
  * Expose some common operations on MLMatrices.
  * Especially, provide matrix multiplication implementation for CSC format sparse matrices.
  */

object LocalMatrix {

  def aggregate(mat: MLMatrix, agg: (Double, Double) => Double): Double = {
    if (mat == null) {
      throw new SparkException("matrix cannot be null for aggregation")
    } else {
      mat match {
        case den: DenseMatrix =>
         den.values.reduce(agg)
        case sp: SparseMatrix =>
         sp.values.reduce(agg)
        case _ => throw new SparkException("Illegal matrix type")
      }
    }
  }

  def compute(a: MLMatrix, b: MLMatrix, f: (Double, Double) => Double): MLMatrix = {
    if (a != null && b != null) {
      /*require(a.numRows == b.numRows,
        s"Matrix A and B must have the same number of rows. But found " +
          s"A.numRows = ${a.numRows}, B.numRows = ${b.numRows}")
      require(a.numCols == b.numCols,
        s"Matrix A and B must have the same number of cols. But found " +
          s"A.numCols = ${a.numCols}, B.numCols = ${b.numCols}")*/
      (a, b) match { // Notice, the size of a and b may be different!!!
        case (ma: DenseMatrix, mb: DenseMatrix) => computeDense(ma, mb, f)
        case (ma: DenseMatrix, mb: SparseMatrix) => computeDenseSparse(ma, mb, f)
        case (ma: SparseMatrix, mb: DenseMatrix) => computeDenseSparse(mb, ma, f)
        case (ma: SparseMatrix, mb: SparseMatrix) => computeSparseSparse(ma, mb, f)
      }
    } else { // return a null value for the invalid case
      null
    }
  }

  def add(a: MLMatrix, b: MLMatrix): MLMatrix = {
    if (a != null && b != null) {
      require(a.numRows == b.numRows,
        s"Matrix A and B must have the same number of rows. But found " +
        s"A.numRows = ${a.numRows}, B.numRows = ${b.numRows}")
      require(a.numCols == b.numCols,
        s"Matrix A and B must have the same number of cols. But found " +
        s"A.numCols = ${a.numCols}, B.numCols = ${b.numCols}")
      (a, b) match {
        case (ma: DenseMatrix, mb: DenseMatrix) => addDense(ma, mb)
        case (ma: DenseMatrix, mb: SparseMatrix) => addDenseSparse(ma, mb)
        case (ma: SparseMatrix, mb: DenseMatrix) => addDenseSparse(mb, ma)
        case (ma: SparseMatrix, mb: SparseMatrix) => addSparseSparse(ma, mb)
      }
    }
    else {
      if (a != null && b == null) a
      else if (a == null && b != null) b
      else null
    }
  }

  private def computeDense(ma: DenseMatrix, mb: DenseMatrix,
                           f: (Double, Double) => Double): MLMatrix = {
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(math.min(arr1.length, arr2.length))(0.0)
    for (i <- 0 until arr.length) {
      arr(i) = f(arr1(i), arr2(i))
    }
    new DenseMatrix(math.min(ma.numRows, mb.numRows),
      math.min(ma.numCols, mb.numCols), arr)
  }

  private def addDense(ma: DenseMatrix, mb: DenseMatrix): MLMatrix = {
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(arr1.length)(0.0)
    for (i <- 0 until arr.length) {
      arr(i) = arr1(i) + arr2(i)
    }
    new DenseMatrix(ma.numRows, ma.numCols, arr)
  }

  private def computeDenseSparse(ma: DenseMatrix, mb: SparseMatrix,
                                 f: (Double, Double) => Double): MLMatrix = {
    val rand = new scala.util.Random()
    if (math.abs(f(rand.nextDouble(), 0.0)) > 1e-6) { // no zero-preserving property on f()
      val (arr1, arr2) = (ma.toArray, mb.toArray)
      val arr = Array.fill(math.min(arr1.length, arr2.length))(0.0)
      for (i <- 0 until arr.length) {
        arr(i) = f(arr1(i), arr2(i))
      }
      new DenseMatrix(math.min(ma.numRows, mb.numRows),
        math.min(ma.numCols, mb.numCols), arr)
    } else {
      if (ma.numRows * ma.numCols <= mb.numRows * mb.numCols) { // sparse matrix input is smaller
        val arr = Array.fill(mb.values.length)(0.0)
        if (!mb.isTransposed) {
          for (k <- 0 until mb.numCols) {
            val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
            for (j <- 0 until cnt) {
              val ind = mb.colPtrs(k) + j
              val rid = mb.rowIndices(ind)
              arr(ind) = f(ma(rid, k), mb.values(ind))
            }
          }
        } else {
          for (k <- 0 until mb.numRows) {
            val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
            for (i <- 0 until cnt) {
              val ind = mb.colPtrs(k) + i
              val cid = mb.rowIndices(ind)
              arr(ind) = f(ma(k, cid), mb.values(ind))
            }
          }
        }
        new SparseMatrix(mb.numRows, mb.numCols, mb.colPtrs, mb.rowIndices, arr, mb.isTransposed)
      } else { // dense matrix is smaller
        val arr = Array.fill(ma.values.length)(0.0)
        val (arr1, arr2) = (ma.toArray, mb.toArray)
        for (i <- 0 until arr.length) {
          if (arr2(i) > 0.0) {
            arr(i) = f(arr1(i), arr2(i))
          }
        }
        new DenseMatrix(ma.numRows, ma.numCols, arr)
      }
    }
  }

  private def addDenseSparse(ma: DenseMatrix, mb: SparseMatrix): MLMatrix = {
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(arr1.length)(0.0)
    for (i <- 0 until arr.length) {
      arr(i) = arr1(i) + arr2(i)
    }
    new DenseMatrix(ma.numRows, ma.numCols, arr)
  }

  // This is an optimized implementation for sparse computation on user-defined functions.
  private def computeSparseSparse(ma: SparseMatrix, mb: SparseMatrix,
                                  f: (Double, Double) => Double): MLMatrix = {
    val rand = new scala.util.Random()
    if (math.abs(f(rand.nextDouble(), 0.0)) > 1e-6 &&
      math.abs(f(0.0, rand.nextDouble())) > 1e-6 ) { // no zero-preserving property on f()
      val (arr1, arr2) = (ma.toArray, mb.toArray)
      val arr = Array.fill(math.min(arr1.length, arr2.length))(0.0)
      var nnz = 0
      for (i <- 0 until arr.length) {
        arr(i) = f(arr1(i), arr2(i))
        if (arr(i) != 0) nnz += 1
      }
      val c = new DenseMatrix(math.min(ma.numRows, mb.numRows),
        math.min(ma.numCols, mb.numCols), arr)
      if (c.numRows * c.numCols > nnz * 2 + c.numCols + 1) {
        c.toSparse
      }
      else {
        c
      }
    } else {
      if (math.abs(f(0.0, rand.nextDouble())) > 1e-6) { // zero-preserving on the left
        if (ma.numRows * ma.numCols <= mb.numRows * mb.numCols) {
          val arr = Array.fill(ma.values.length)(0.0)
          if (!ma.isTransposed) {
            for (k <- 0 until ma.numCols) {
              val cnt = ma.colPtrs(k + 1) - ma.colPtrs(k)
              for (j <- 0 until cnt) {
                val ind = ma.colPtrs(k) + j
                val rid = ma.rowIndices(ind)
                arr(ind) = f(ma.values(ind), mb(rid, k))
              }
            }
          } else {
            for (k <- 0 until ma.numRows) {
              val cnt = ma.colPtrs(k + 1) - ma.colPtrs(k)
              for (i <- 0 until cnt) {
                val ind = ma.colPtrs(k) + i
                val cid = ma.rowIndices(ind)
                arr(ind) = f(ma.values(ind), mb(k, cid))
              }
            }
          }
          new SparseMatrix(ma.numRows, ma.numCols, ma.colPtrs, ma.rowIndices, arr, ma.isTransposed)
        } else {
          val arr = Array.fill(mb.values.length)(0.0)
          if (!mb.isTransposed) {
            for (k <- 0 until mb.numCols) {
              val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
              for (j <- 0 until cnt) {
                val ind = mb.colPtrs(k) + j
                val rid = mb.rowIndices(ind)
                val mav = ma(rid, k)
                if (mav > 0.0) {
                  arr(ind) = f(mav, mb.values(ind))
                }
              }
            }
          } else {
            for (k <- 0 until mb.numRows) {
              val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
              for (i <- 0 until cnt) {
                val ind = mb.colPtrs(k) + i
                val cid = mb.rowIndices(ind)
                val mav = ma(k, cid)
                if (mav > 0.0) {
                  arr(ind) = f(mav, mb.values(ind))
                }
              }
            }
          }
          new SparseMatrix(mb.numRows, mb.numCols, mb.colPtrs, mb.rowIndices, arr, mb.isTransposed)
        }
      } else { // zero-preserving on the right
        if (ma.numRows * ma.numCols >= mb.numRows * mb.numCols) {
          val arr = Array.fill(mb.values.length)(0.0)
          if (!mb.isTransposed) {
            for (k <- 0 until mb.numCols) {
              val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
              for (j <- 0 until cnt) {
                val ind = mb.colPtrs(k) + j
                val rid = mb.rowIndices(ind)
                arr(ind) = f(ma(rid, k), mb.values(ind))
              }
            }
          } else {
            for (k <- 0 until mb.numRows) {
              val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
              for (i <- 0 until cnt) {
                val ind = mb.colPtrs(k) + i
                val cid = mb.rowIndices(ind)
                arr(ind) = f(ma(k, cid), mb.values(ind))
              }
            }
          }
          new SparseMatrix(mb.numRows, mb.numCols, mb.colPtrs, mb.rowIndices, arr, mb.isTransposed)
        } else {
          val arr = Array.fill(ma.values.length)(0.0)
          if (!ma.isTransposed) {
            for (k <- 0 until ma.numCols) {
              val cnt = ma.colPtrs(k + 1) - ma.colPtrs(k)
              for (j <- 0 until cnt) {
                val ind = ma.colPtrs(k) + j
                val rid = ma.rowIndices(ind)
                val mbv = mb(rid, k)
                if (mbv > 0.0) {
                  arr(ind) = f(ma.values(ind), mbv)
                }
              }
            }
          } else {
            for (k <- 0 until ma.numRows) {
              val cnt = ma.colPtrs(k + 1) - ma.colPtrs(k)
              for (i <- 0 until cnt) {
                val ind = ma.colPtrs(k) + i
                val cid = ma.rowIndices(ind)
                val mbv = mb(k, cid)
                if (mbv > 0.0) {
                  arr(ind) = f(ma.values(ind), mbv)
                }
              }
            }
          }
          new SparseMatrix(ma.numRows, ma.numCols, ma.colPtrs, ma.rowIndices, arr, ma.isTransposed)
        }
      }
    }
  }

  private def addSparseSparse(ma: SparseMatrix, mb: SparseMatrix): MLMatrix = {
    if (ma.isTransposed || mb.isTransposed) {
      val (arr1, arr2) = (ma.toArray, mb.toArray)
      val arr = Array.fill(arr1.length)(0.0)
      var nnz = 0
      for (i <- 0 until arr.length) {
        arr(i) = arr1(i) + arr2(i)
        if (arr(i) != 0) nnz += 1
      }
      val c = new DenseMatrix(ma.numRows, ma.numCols, arr)
      if (c.numRows * c.numCols > nnz * 2 + c.numCols + 1) {
        c.toSparse
      }
      else {
        c
      }
    }
    else {
      addSparseSparseNative(ma, mb)
    }
  }

  // add two sparse matrices in CSC format together
  // without converting them into dense matrix format
  private def addSparseSparseNative(ma: SparseMatrix, mb: SparseMatrix): MLMatrix = {
    require(ma.numRows == mb.numRows, s"Matrix A.numRows must be equal to B.numRows, but found " +
      s"A.numRows = ${ma.numRows}, B.numRows = ${mb.numRows}")
    require(ma.numCols == mb.numCols, s"Matrix A.numCols must be equal to B.numCols, but found " +
      s"A.numCols = ${ma.numCols}, B.numCols = ${mb.numCols}")
    val va = ma.values
    val rowIdxa = ma.rowIndices
    val colPtra = ma.colPtrs
    val vb = mb.values
    val rowIdxb = mb.rowIndices
    val colPtrb = mb.colPtrs
    val vc = ArrayBuffer[Double]()
    val rowIdxc = ArrayBuffer[Int]()
    val colPtrc = new Array[Int](ma.numCols + 1)
    val coltmp = new Array[Double](ma.numRows)
    for (jc <- 0 until ma.numCols) {
      val numPerColb = colPtrb(jc + 1) - colPtrb(jc)
      val numPerCola = colPtra(jc + 1) - colPtra(jc)
      arrayClear(coltmp)
      for (ia <- colPtra(jc) until colPtra(jc) + numPerCola) {
        coltmp(rowIdxa(ia)) += va(ia)
      }
      for (ib <- colPtrb(jc) until colPtrb(jc) + numPerColb) {
        coltmp(rowIdxb(ib)) += vb(ib)
      }
      var count = 0
      for (i <- 0 until coltmp.length) {
        if (coltmp(i) != 0.0) {
          rowIdxc += i
          vc += coltmp(i)
          count += 1
        }
      }
      colPtrc(jc + 1) = colPtrc(jc) + count
    }
    if (ma.numRows * ma.numCols > 2 * colPtrc(colPtrc.length - 1) + colPtrc.length) {
      new SparseMatrix(ma.numRows, ma.numCols, colPtrc, rowIdxc.toArray, vc.toArray)
    }
    else {
      new SparseMatrix(ma.numRows, ma.numCols, colPtrc, rowIdxc.toArray, vc.toArray).toDense
    }
  }

  // provides a native implementation for CSC sparse matrix multiplication
  // instead of converting to a dense matrix for multiplication
  def multiplySparseSparse(ma: SparseMatrix, mb: SparseMatrix): MLMatrix = {
    require(ma.numCols == mb.numRows, s"Matrix A.numCols must be equals to B.numRows, but found " +
      s"A.numCols = ${ma.numCols}, B.numRows = ${mb.numRows}")
    (ma.isTransposed, mb.isTransposed) match {
      case (false, false) => multiplyCSC_CSC(ma, mb)
      case (true, true) => multiplyCSR_CSR(ma, mb)
      case (true, false) => multiplyCSR_CSC(ma, mb)
      case (false, true) => multiplyCSC_CSR(ma, mb)
    }
  }

  // stores in CSC format
  private def multiplyCSC_CSC(ma: SparseMatrix, mb: SparseMatrix): MLMatrix = {
    val va = ma.values
    val rowIdxa = ma.rowIndices
    val colPtra = ma.colPtrs
    val vb = mb.values
    val rowIdxb = mb.rowIndices
    val colPtrb = mb.colPtrs
    val vc = new ArrayBuffer[Double]()
    val rowIdxc = new ArrayBuffer[Int]()
    val colPtrc = new Array[Int](mb.numCols + 1)
    val coltmp = new Array[Double](ma.numRows)
    for (jb <- 0 until mb.numCols) {
      val numPerColb = colPtrb(jb + 1) - colPtrb(jb)
      arrayClear(coltmp)
      for(ib <- colPtrb(jb) until colPtrb(jb) + numPerColb) {
        val alpha = vb(ib)
        // alpha * the column with idx of rowIdxb(ib)
        val ja = rowIdxb(ib)
        val numPerCola = colPtra(ja + 1) - colPtra(ja)
        for (ia <- colPtra(ja) until colPtra(ja) + numPerCola) {
          val idx = rowIdxa(ia)
          coltmp(idx) += va(ia) * alpha
        }
      }
      var count = 0
      for (i <- 0 until coltmp.length) {
        if (coltmp(i) != 0.0) {
          rowIdxc += i
          vc += coltmp(i)
          count += 1
        }
      }
      colPtrc(jb + 1) = colPtrc(jb) + count
    }
    if (ma.numRows * mb.numCols > 2 * colPtrc(colPtrc.length - 1) + colPtrc.length) {
      new SparseMatrix(ma.numRows, mb.numCols, colPtrc, rowIdxc.toArray, vc.toArray)
    }
    else {
      new SparseMatrix(ma.numRows, mb.numCols, colPtrc, rowIdxc.toArray, vc.toArray).toDense
    }
  }

  // stores in CSR format
  private def multiplyCSR_CSR(ma: SparseMatrix, mb: SparseMatrix): MLMatrix = {
    val va = ma.values
    val colIdxa = ma.rowIndices
    val rowPtra = ma.colPtrs
    val vb = mb.values
    val colIdxb = mb.rowIndices
    val rowPtrb = mb.colPtrs
    val vc = new ArrayBuffer[Double]()
    val colIdxc = new ArrayBuffer[Int]()
    val rowPtrc = new Array[Int](ma.numRows + 1)
    val rowtmp = new Array[Double](mb.numCols)
    for (ia <- 0 until ma.numRows) {
      val numPerRowa = rowPtra(ia + 1) - rowPtra(ia)
      arrayClear(rowtmp)
      for (ja <- rowPtra(ia) until rowPtra(ia) + numPerRowa) {
        val alpha = va(ja)
        // alpha * the row with idx of colIdxa(ja)
        val ib = colIdxa(ja)
        val numPerRowb = rowPtrb(ib + 1) - rowPtrb(ib)
        for (jb <- rowPtrb(ib) until rowPtrb(ib) + numPerRowb) {
          val idx = colIdxb(jb)
          rowtmp(idx) += vb(jb) * alpha
        }
      }
      var count = 0
      for (i <- 0 until rowtmp.length) {
        if (rowtmp(i) != 0.0) {
          colIdxc += i
          vc += rowtmp(i)
          count += 1
        }
      }
      rowPtrc(ia + 1) = rowPtrc(ia) + count
    }
    if (ma.numRows * mb.numCols > 2 * rowPtrc(rowPtrc.length - 1) + rowPtrc.length) {
      new SparseMatrix(ma.numRows, mb.numCols, rowPtrc, colIdxc.toArray, vc.toArray, true)
    }
    else {
      new SparseMatrix(ma.numRows, mb.numCols, rowPtrc, colIdxc.toArray, vc.toArray, true).toDense
    }
  }

  // stores in CSC format, proceeds in column-by-column fashion
  private def multiplyCSR_CSC(ma: SparseMatrix, mb: SparseMatrix): MLMatrix = {
    val va = ma.values
    val colIdxa = ma.rowIndices
    val rowPtra = ma.colPtrs
    val vb = mb.values
    val rowIdxb = mb.rowIndices
    val colPtrb = mb.colPtrs
    val vc = new ArrayBuffer[Double]()
    val rowIdxc = new ArrayBuffer[Int]()
    val colPtrc = new Array[Int](mb.numCols + 1)
    val coltmp = new Array[Double](ma.numRows)
    for (jb <- 0 until colPtrb.length - 1) {
      val num = colPtrb(jb + 1) - colPtrb(jb)
      val entryColb = new mutable.HashMap[Int, Double]() // store the entries in jb-th col of B
      for (ib <- colPtrb(jb) until colPtrb(jb) + num) {
        entryColb.put(rowIdxb(ib), vb(ib))
      }
      arrayClear(coltmp)
      // scan each row of matrix A for multiplication
      for (ia <- 0 until rowPtra.length - 1) {
        val count = rowPtra(ia + 1) - rowPtra(ia)
        for (ja <- rowPtra(ia) until rowPtra(ia) + count) {
          if (entryColb.contains(colIdxa(ja))) {
            coltmp(ia) += va(ja) * entryColb.get(colIdxa(ja)).get
          }
        }
      }
      var count = 0
      for (i <- 0 until coltmp.length) {
        if (coltmp(i) != 0.0) {
          count += 1
          vc += coltmp(i)
          rowIdxc += i
        }
      }
      colPtrc(jb + 1) = colPtrc(jb) + count
    }
    if (ma.numRows * mb.numCols > 2 * colPtrc(colPtrc.length - 1) + colPtrc.length) {
      new SparseMatrix(ma.numRows, mb.numCols, colPtrc, rowIdxc.toArray, vc.toArray)
    }
    else {
      new SparseMatrix(ma.numRows, mb.numCols, colPtrc, rowIdxc.toArray, vc.toArray)
    }
  }

  // stores in CSC format, proceeds in outer product fashion
  // k-th col of A multiply with k-th row of B
  private def multiplyCSC_CSR(ma: SparseMatrix, mb: SparseMatrix): MLMatrix = {
    // reserve a buffer for the worse case
    val buf = new Array[Double](ma.numRows * mb.numCols)
    val n = ma.numRows
    // C(i, j) --> buf(i + j*n)
    val va = ma.values
    val rowIdxa = ma.rowIndices
    val colPtra = ma.colPtrs
    val vb = mb.values
    val colIdxb = mb.rowIndices
    val rowPtrb = mb.colPtrs
    for (ja <- 0 until colPtra.length - 1) {
      // ja-th col of A
      val counta = colPtra(ja + 1) - colPtra(ja)
      val rowValMap = new mutable.HashMap[Int, Double]()
      for (ia <- colPtra(ja) until colPtra(ja) + counta) {
        rowValMap.put(rowIdxa(ia), va(ia))
      }
      // get ja-th row of B
      val countb = rowPtrb(ja + 1) - rowPtrb(ja)
      for (jb <- rowPtrb(ja) until rowPtrb(ja) + countb) {
        for (elem <- rowValMap) {
          val i = elem._1
          val j = colIdxb(jb)
          buf(i + j*n) += elem._2 * vb(jb)
        }
      }
    }
    val nnz = buf.count(x => x != 0.0)
    if (ma.numRows * mb.numCols <= 2 * nnz + mb.numCols) {
      new DenseMatrix(ma.numRows, mb.numCols, buf)
    }
    else {
      new DenseMatrix(ma.numRows, mb.numCols, buf).toSparse
    }
  }

  /*
   * Convert a CSR format sparse matrix to a CSC format sparse matrix.
   * Output the three vectors for CSC format, (rowIdx, colPtr, values)
   */
  def CSR2SCS(sp: SparseMatrix): (Array[Int], Array[Int], Array[Double]) = {
    require(sp.isTransposed, s"Sparse matrix needs to set true for isTransposed bit, but found " +
      s"sp.isTransposed = ${sp.isTransposed}")
    val vcsr = sp.values
    val colIdxcsr = sp.rowIndices
    val rowPtrcsr = sp.colPtrs
    val values = new Array[Double](sp.values.length)
    val rowIdx = new Array[Int](sp.values.length)
    val colPtr = new Array[Int](sp.numCols + 1)
    val colMap = scala.collection.mutable.LinkedHashMap[Int, ArrayBuffer[Int]]()
    for (i <- 0 until colIdxcsr.length) {
      val elem = colMap.getOrElse(colIdxcsr(i), new ArrayBuffer[Int]())
      elem += i
      colMap(colIdxcsr(i)) = elem   // update for the map
    }
    var idx = 0
    for (c <- 0 until colPtr.length - 1) {
      colPtr(c + 1) = colPtr(c) + colMap(c).length
      for (id <- colMap(c)) {
        values(idx) = vcsr(id)
        rowIdx(idx) = binSearch(rowPtrcsr, id)
        idx += 1
      }
    }

    (rowIdx, colPtr, values)
  }

  /*
   * Convert a CSC format sparse matrix to a CSR format sparse matrix.
   * Ouput the three vectors for CSR format, (rowPtr, colIdx, values)
   */
  def CSC2CSR(sp: SparseMatrix): (Array[Int], Array[Int], Array[Double]) = {
    require(!sp.isTransposed, s"Sparse matrix needs to set false for isTransposed bit, but found " +
      s"sp.isTransposed = ${sp.isTransposed}")
    val vcsc = sp.values
    val rowIdxcsc = sp.rowIndices
    val colPtrcsc = sp.colPtrs
    val values = new Array[Double](vcsc.length)
    val colIdx = new Array[Int](vcsc.length)
    val rowPtr = new Array[Int](sp.numRows + 1)
    val rowMap = scala.collection.mutable.LinkedHashMap[Int, ArrayBuffer[Int]]()
    for (i <- 0 until rowIdxcsc.length) {
      val elem = rowMap.getOrElse(rowIdxcsc(i), new ArrayBuffer[Int]())
      elem += i
      rowMap(rowIdxcsc(i)) = elem
    }
    var idx = 0
    for (r <- 0 until rowPtr.length - 1) {
      rowPtr(r + 1) = rowPtr(r) + rowMap(r).length
      for (id <- rowMap(r)) {
        values(idx) = vcsc(id)
        colIdx(idx) = binSearch(colPtrcsc, id)
        idx += 1
      }
    }
    (rowPtr, colIdx, values)
  }

  private def binSearch(arr: Array[Int], target: Int): Int = {
    var (lo, hi) = (0, arr.length - 2)
    while (lo <= hi) {
      val mid = lo + (hi - lo) / 2
      if (arr(mid) == target || (arr(mid) < target && target < arr(mid + 1))) {
        return mid
      }
      else if (arr(mid) > target) {
        hi = mid - 1
      }
      else {
        lo = mid + 1
      }
    }
    throw new SparkException("binary search failed for CSR2CSC/CSC2CSR conversion!")
  }

  private def arrayClear(arr: Array[Double]): Unit = {
    for (i <- 0 until arr.length) {
      arr(i) = 0.0
    }
  }

  def multiplyScalar(alpha: Double, a: MLMatrix): MLMatrix = {
    a match {
      case ma: DenseMatrix => multiplyScalarDense(alpha, ma)
      case ma: SparseMatrix => multiplyScalarSparse(alpha, ma)
    }
  }

  private def multiplyScalarDense(alpha: Double, ma: DenseMatrix): MLMatrix = {
    val arr = for (elem <- ma.values) yield alpha * elem
    new DenseMatrix(ma.numRows, ma.numCols, arr, ma.isTransposed)
  }

  private def multiplyScalarSparse(alpha: Double, ma: SparseMatrix): MLMatrix = {
    val arr = for (elem <- ma.values) yield alpha * elem
    new SparseMatrix(ma.numRows, ma.numCols, ma.colPtrs, ma.rowIndices, arr, ma.isTransposed)
  }

  def toBreeze(mat: MLMatrix): BM[Double] = {
    mat match {
      case dm: DenseMatrix => denseToBreeze(dm)
      case sp: SparseMatrix => sparseToBreeze(sp)
    }
  }

  private def denseToBreeze(mat: DenseMatrix): BM[Double] = {
    if (!mat.isTransposed) {
      new BDM[Double](mat.numRows, mat.numCols, mat.values)
    }
    else {
      val breezeMatrix = new BDM[Double](mat.numRows, mat.numCols, mat.values)
      breezeMatrix.t
    }
  }

  private def sparseToBreeze(mat: SparseMatrix): BM[Double] = {
    if (!mat.isTransposed) {
      new BSM[Double](mat.values, mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices)
    }
    else {
      val breezeMatrix = new BSM[Double](
        mat.values, mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices)
      breezeMatrix.t
    }
  }

  def fromBreeze(breeze: BM[Double]): MLMatrix = {
    breeze match {
      case dm: BDM[Double] => new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
      case sp: BSM[Double] =>
        new SparseMatrix(sp.rows, sp.cols, sp.colPtrs, sp.rowIndices, sp.data)
      case _ => throw new UnsupportedOperationException(
        s"Do not support conversion from type ${breeze.getClass.getName}")
    }
  }

  def elementWiseMultiply(mat1: MLMatrix, mat2: MLMatrix): MLMatrix = {
    require(mat1.numRows == mat2.numRows,
      s"mat1.numRows = ${mat1.numRows}, mat2.numRows = ${mat2.numRows}")
    require(mat1.numCols == mat2.numCols,
      s"mat1.numCols = ${mat1.numCols}, mat2.numCols = ${mat2.numCols}")
    (mat1, mat2) match {
      case (ma: DenseMatrix, mb: DenseMatrix) => elementWiseOpDenseDense(ma, mb)
      case (ma: DenseMatrix, mb: SparseMatrix) => elementWiseOpDenseSparse(ma, mb)
      case (ma: SparseMatrix, mb: DenseMatrix) => elementWiseOpDenseSparse(mb, ma)
      case (ma: SparseMatrix, mb: SparseMatrix) => elementWiseOpSparseSparse(ma, mb)
    }
  }

  def elementWiseDivide(mat1: MLMatrix, mat2: MLMatrix): MLMatrix = {
    require(mat1.numRows == mat2.numRows,
      s"mat1.numRows = ${mat1.numRows}, mat2.numRows = ${mat2.numRows}")
    require(mat1.numCols == mat2.numCols,
      s"mat1.numCols = ${mat1.numCols}, mat2.numCols = ${mat2.numCols}")
    (mat1, mat2) match {
      case (ma: DenseMatrix, mb: DenseMatrix) => elementWiseOpDenseDense(ma, mb, 1)
      case (ma: DenseMatrix, mb: SparseMatrix) => elementWiseOpDenseSparse(ma, mb, 1)
      case (ma: SparseMatrix, mb: DenseMatrix) => elementWiseOpDenseSparse(mb, ma, 1)
      case (ma: SparseMatrix, mb: SparseMatrix) => elementWiseOpSparseSparse(ma, mb, 1)
    }
  }

  // op = 0 -- multiplication, op = 1 -- division
  private def elementWiseOpDenseDense(ma: DenseMatrix, mb: DenseMatrix, op: Int = 0) = {
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(arr1.length)(0.0)
    for (i <- 0 until arr.length) {
      if (op == 0) {
        arr(i) = arr1(i) * arr2(i)
      }
      else {
        arr(i) = arr1(i) / arr2(i)
      }
    }
    new DenseMatrix(ma.numRows, ma.numCols, arr)
  }

  private def elementWiseOpDenseSparse(ma: DenseMatrix, mb: SparseMatrix, op: Int = 0) = {
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(arr1.length)(0.0)
    for (i <- 0 until arr.length) {
      if (op == 0) {
        arr(i) = arr1(i) * arr2(i)
      }
      else {
        arr(i) = arr1(i) / arr2(i)
      }
    }
    new DenseMatrix(ma.numRows, ma.numCols, arr)
  }

  private def elementWiseOpSparseSparse(ma: SparseMatrix, mb: SparseMatrix, op: Int = 0) = {
    if (ma.isTransposed || mb.isTransposed) {
      val (arr1, arr2) = (ma.toArray, mb.toArray)
      val arr = Array.fill(arr1.length)(0.0)
      var nnz = 0
      for (i <- 0 until arr.length) {
        if (op == 0) {
          arr(i) = arr1(i) * arr2(i)
        }
        else {
          arr(i) = arr1(i) / arr2(i)
        }
        if (arr(i) != 0) nnz += 1
      }
      val c = new DenseMatrix(ma.numRows, ma.numCols, arr)
      if (c.numRows * c.numCols > nnz * 2 + c.numCols + 1) {
        c.toSparse
      }
      else {
        c
      }
    }
    else {
      elementWiseOpSparseSparseNative(ma, mb, op)
    }
  }

  def elementWiseOpSparseSparseNative(ma: SparseMatrix,
                                      mb: SparseMatrix, op: Int = 0): MLMatrix = {
    require(ma.numRows == mb.numRows,
      s"Matrix A.numRows must be equal to B.numRows, but found " +
      s"A.numRows = ${ma.numRows}, B.numRows = ${mb.numRows}")
    require(ma.numCols == mb.numCols,
      s"Matrix A.numCols must be equal to B.numCols, but found " +
      s"A.numCols = ${ma.numCols}, B.numCols = ${mb.numCols}")
    val va = ma.values
    val rowIdxa = ma.rowIndices
    val colPtra = ma.colPtrs
    val vb = mb.values
    val rowIdxb = mb.rowIndices
    val colPtrb = mb.colPtrs
    val vc = ArrayBuffer[Double]()
    val rowIdxc = ArrayBuffer[Int]()
    val colPtrc = new Array[Int](ma.numCols + 1)
    val coltmp1 = new Array[Double](ma.numRows)
    val coltmp2 = new Array[Double](mb.numRows)
    for (jc <- 0 until ma.numCols) {
      val numPerColb = colPtrb(jc + 1) - colPtrb(jc)
      val numPerCola = colPtra(jc + 1) - colPtra(jc)
      arrayClear(coltmp1)
      arrayClear(coltmp2)
      for (ia <- colPtra(jc) until colPtra(jc) + numPerCola) {
        coltmp1(rowIdxa(ia)) += va(ia)
      }
      for (ib <- colPtrb(jc) until colPtrb(jc) + numPerColb) {
        coltmp2(rowIdxb(ib)) += vb(ib)
      }
      for (ix <- 0 until coltmp1.length) {
        if (op == 0) {
          coltmp1(ix) *= coltmp2(ix)
        }
        else {
          coltmp1(ix) /= coltmp2(ix)
        }
      }
      var count = 0
      for (i <- 0 until coltmp1.length) {
        if (coltmp1(i) != 0.0) {
          rowIdxc += i
          vc += coltmp1(i)
          count += 1
        }
      }
      colPtrc(jc + 1) = colPtrc(jc) + count
    }
    if (ma.numRows * ma.numCols > 2 * colPtrc(colPtrc.length - 1) + colPtrc.length) {
      new SparseMatrix(ma.numRows, ma.numCols, colPtrc, rowIdxc.toArray, vc.toArray)
    }
    else {
      new SparseMatrix(ma.numRows, ma.numCols, colPtrc, rowIdxc.toArray, vc.toArray).toDense
    }
  }

  // C = C + A * B, and C is pre-allocated already
  // this method should save memory when C is reused for later iterations
  // cannot handle the case when C is a sparse matrix and holding a non-exsitent
  // entry for the product
  def incrementalMultiply(A: MLMatrix, B: MLMatrix, C: MLMatrix): MLMatrix = {
    require(A.numRows == C.numCols && B.numCols == C.numCols, s"dimension mismatch " +
      s"A.numRows = ${A.numRows}, C.numRows = ${C.numRows}," +
      s"B.numCols = ${B.numCols}, C.numCols = ${C.numCols}")
    (A, B, C) match {
      case (matA: DenseMatrix, matB: DenseMatrix, matC: DenseMatrix) =>
        incrementalMultiplyDenseDense(matA, matB, matC)
      case (matA: DenseMatrix, matB: SparseMatrix, matC: DenseMatrix) =>
        incrementalMultiplyDenseSparse(matA, matB, matC)
      case (matA: SparseMatrix, matB: DenseMatrix, matC: DenseMatrix) =>
        incrementalMultiplySparseDense(matA, matB, matC)
      case (matA: SparseMatrix, matB: SparseMatrix, matC: DenseMatrix) =>
        incrementalMultiplySparseSparseDense(matA, matB, matC)
      // case (matA: SparseMatrix, matB: SparseMatrix, matC: SparseMatrix) =>
      case _ => new SparkException(s"incrememtalMultiply does not apply to " +
        s"the required format of matrices, " +
        s"A: ${A.getClass}, B: ${B.getClass}, C: ${C.getClass}")
    }
    C
  }

  private def incrementalMultiplyDenseDense(A: DenseMatrix, B: DenseMatrix, C: DenseMatrix) = {
    for (i <- 0 until A.numRows) {
      for (j <- 0 until B.numCols) {
        for (k <- 0 until A.numCols) {
          var v1 = 0.0
          if (A.isTransposed) v1 = A(k, i)
          else v1 = A(i, k)
          var v2 = 0.0
          if (B.isTransposed) v2 = B(j, k)
          else v2 = B(k, j)
          if (!C.isTransposed) {
            C(i, j) += v1 * v2
          }
          else {
            C(j, i) += v1 * v2
          }
        }
      }
    }
  }

  private def incrementalMultiplyDenseSparse(A: DenseMatrix, B: SparseMatrix, C: DenseMatrix) = {
    var values: Array[Double] = B.values
    var rowIdx: Array[Int] = B.rowIndices
    var colPtr: Array[Int] = B.colPtrs
    if (B.isTransposed) {
      val res = CSR2SCS(B)
      rowIdx = res._1
      colPtr = res._2
      values = res._3
    }
    // multiply a matrix from right-hand-side is equvilent to perform column transformantion
    // on the first matrix
    for (c <- 0 until B.numCols) {
      val count = colPtr(c + 1) - colPtr(c)
      val cstart = colPtr(c)
      for (ridx <- cstart until cstart + count) {
        val v = values(ridx)
        val r = rowIdx(ridx)
        // r-th column of A multiply with v and add those values to c-th column in C
        for (i <- 0 until A.numRows) {
          var va = 0.0
          if (A.isTransposed) va = A(r, i)
          else va = A(i, r)
          if (!C.isTransposed) {
            C(i, c) += va * v
          }
          else {
            C(c, i) += va * v
          }
        }
      }
    }
  }

  private def incrementalMultiplySparseDense(A: SparseMatrix, B: DenseMatrix, C: DenseMatrix) = {
    var values: Array[Double] = A.values
    var colIdx: Array[Int] = A.rowIndices
    var rowPtr: Array[Int] = A.colPtrs
    if (!A.isTransposed) {
      val res = CSC2CSR(A)
      rowPtr = res._1
      colIdx = res._2
      values = res._3
    }
    // multiply a sparse matrix from left-hand-side is equivalent to perform row transformation
    // on the second matrix
    for (r <- 0 until A.numRows) {
      val count = rowPtr(r + 1) - rowPtr(r)
      val rstart = rowPtr(r)
      for (cidx <- rstart until rstart + count) {
        val v = values(cidx)
        val c = colIdx(cidx)
        // c-th row of B mutliply with v and add those tho the r-th row in C
        for (j <- 0 until B.numCols) {
          var vb = 0.0
          if (B.isTransposed) vb = B(j, c)
          else vb = B(c, j)
          if (!C.isTransposed) {
            C(r, j) += v * vb
          }
          else {
            C(j, r) += v * vb
          }
        }
      }
    }
  }

  private def incrementalMultiplySparseSparseDense(A: SparseMatrix,
                                                   B: SparseMatrix,
                                                   C: DenseMatrix) = {
    if (!A.isTransposed && !B.isTransposed) { // both A and B are not transposed
    // process as column transformation on A
    val valuesb = B.values
      val rowIdxb = B.rowIndices
      val colPtrb = B.colPtrs
      val valuesa = A.values
      val rowIdxa = A.rowIndices
      val colPtra = A.colPtrs
      for (cb <- 0 until B.numCols) {
        val countb = colPtrb(cb + 1) - colPtrb(cb)
        val startb = colPtrb(cb)
        for (ridxb <- startb until startb + countb) {
          val vb = valuesb(ridxb)
          val rowb = rowIdxb(ridxb)
          // get rowb-th column from A and multiply it with vb
          val counta = colPtra(rowb + 1) - colPtra(rowb)
          val starta = colPtra(rowb)
          for (ridxa <- starta until starta + counta) {
            val va = valuesa(ridxa)
            val rowa = rowIdxa(ridxa)
            if (!C.isTransposed) {
              C(rowa, cb) += va * vb
            }
            else {
              C(cb, rowa) += va * vb
            }
          }
        }
      }
    }
    else if (A.isTransposed && B.isTransposed) { // both A and B are transposed
    // process as row transformation on B
    val valuesa = A.values
      val colIdxa = A.rowIndices
      val rowPtra = A.colPtrs
      val valuesb = B.values
      val colIdxb = B.rowIndices
      val rowPtrb = B.colPtrs
      for (ra <- 0 until A.numRows) {
        val counta = rowPtra(ra + 1) - rowPtra(ra)
        val starta = rowPtra(ra)
        for (cidxa <- starta until starta + counta) {
          val va = valuesa(cidxa)
          val cola = colIdxa(cidxa)
          // get cola-th row from B and multiply it with va
          val countb = rowPtrb(cola + 1) - rowPtrb(cola)
          val startb = rowPtrb(cola)
          for (cidxb <- startb until startb + countb) {
            val vb = valuesb(cidxb)
            val colb = colIdxb(cidxb)
            if (!C.isTransposed) {
              C(ra, colb) += va * vb
            }
            else {
              C(colb, ra) += va * vb
            }
          }
        }
      }
    }
    else if (A.isTransposed && !B.isTransposed) {
    // A is stored in row format and B is stored in column format
    // this is an easy (natrual) case
    val valuesa = A.values
      val colIdxa = A.rowIndices
      val rowPtra = A.colPtrs
      val valuesb = B.values
      val rowIdxb = B.rowIndices
      val colPtrb = B.colPtrs
      for (ra <- 0 until A.numRows) {
        val counta = rowPtra(ra + 1) - rowPtra(ra)
        val starta = rowPtra(ra)
        val entrya: ArrayBuffer[(Int, Double)] = new ArrayBuffer[(Int, Double)]()
        for (cidxa <- starta until starta + counta) {
          entrya.append((colIdxa(cidxa), valuesa(cidxa)))
        }
        for (cb <- 0 until B.numCols) {
          val countb = colPtrb(cb + 1) - colPtrb(cb)
          val startb = colPtrb(cb)
          val entryb: ArrayBuffer[(Int, Double)] = new ArrayBuffer[(Int, Double)]()
          for (ridxb <- startb until startb + countb) {
            entryb.append((rowIdxb(ridxb), valuesb(ridxb)))
          }
          // check if any entries are matched in entrya and entryb
          var (i, j) = (0, 0)
          while (i < entrya.length && j < entryb.length) {
            if (entrya(i)._1 == entryb(j)._1) {
              if (!C.isTransposed) {
                C(ra, cb) += entrya(i)._2 * entryb(j)._2
              }
              else {
                C(cb, ra) += entrya(i)._2 * entryb(j)._2
              }
              i += 1
              j += 1
            }
            else if (entrya(i)._1 > entryb(j)._1) j += 1
            else i += 1
          }
        }
      }
    }
    else {  // A is stored in column format and B is stored in row format
    // this can be viewed as outer-product of the k-th column from A with the k-th row from B
    val valuesa = A.values
      val rowIdxa = A.rowIndices
      val colPtra = A.colPtrs
      val valuesb = B.values
      val colIdxb = B.rowIndices
      val rowPtrb = B.colPtrs
      for (idx <- 0 until A.numCols) {
        val counta = colPtra(idx + 1) - colPtra(idx)
        val starta = colPtra(idx)
        val entrya: ArrayBuffer[(Int, Double)] = new ArrayBuffer[(Int, Double)]()
        for (ridx <- starta until starta + counta) {
          entrya.append((rowIdxa(ridx), valuesa(ridx)))
        }
        val countb = rowPtrb(idx + 1) - rowPtrb(idx)
        val startb = rowPtrb(idx)
        val entryb: ArrayBuffer[(Int, Double)] = new ArrayBuffer[(Int, Double)]()
        for (cidx <- startb until startb + countb) {
          entryb.append((colIdxb(cidx), valuesb(cidx)))
        }
        for (t1 <- entrya)
          for (t2 <- entryb)
            if (!C.isTransposed) {
              C(t1._1, t2._1) += t1._2 * t2._2
            }
            else {
              C(t2._1, t1._1) += t1._2 * t2._2
            }
      }
    }
  }

  def multiplySparseMatDenseVec(sp: SparseMatrix, dv: DenseMatrix): DenseMatrix = {
    require(dv.numCols == 1, s"vector with more than 1 columns, dv.numCols = ${dv.numCols}")
    require(sp.numCols == dv.numRows, s"Sparse Mat-Vect dimensions do not match, " +
      s"Mat.numCols = ${sp.numCols}, Vec.numRows = ${dv.numRows}")
    val arr = new Array[Double](sp.numRows)
    if (sp.isTransposed) { // ideal condition that sp is in CSR format
    val values = sp.values
      val colIdx = sp.rowIndices
      val rowPtr = sp.colPtrs
      for (ridx <- 0 until arr.length) {
        val count = rowPtr(ridx + 1) - rowPtr(ridx)
        val start = rowPtr(ridx)
        for (cidx <- start until start + count) {
          arr(ridx) += values(cidx) * dv(colIdx(cidx), 0)
        }
      }
      new DenseMatrix(sp.numRows, 1, arr)
    }
    else { // sp is in CSC format
    val values = sp.values
      val rowIdx = sp.rowIndices
      val colPtr = sp.colPtrs
      for (cidx <- 0 until sp.numCols) {
        val count = colPtr(cidx + 1) - colPtr(cidx)
        val start = colPtr(cidx)
        for (ridx <- start until start + count) {
          arr(rowIdx(ridx)) += values(ridx) * dv(cidx, 0)
        }
      }
      new DenseMatrix(sp.numRows, 1, arr)
    }
  }

  def matrixMultiplication(mat1: MLMatrix, mat2: MLMatrix): MLMatrix = {
    (mat1, mat2) match {
      case (dm1: DenseMatrix, dm2: DenseMatrix) => dm1.multiply(dm2)
      case (dm: DenseMatrix, sp: SparseMatrix) => dm.multiply(sp.toDense)
      case (sp: SparseMatrix, dm: DenseMatrix) =>
        if (dm.numCols == 1) { // add support for sparse matrix multiply with dense vector
          LocalMatrix.multiplySparseMatDenseVec(sp, dm)
        }
        else {
          sp.multiply(dm)
        }
      case (sp1: SparseMatrix, sp2: SparseMatrix) =>
        val sparsity1 = sp1.values.length * 1.0 / (sp1.numRows * sp1.numCols)
        val sparsity2 = sp2.values.length * 1.0 / (sp2.numRows * sp2.numCols)
        if (sparsity1 > 0.1) {
          sp1.toDense.multiply(sp2.toDense)
        }
        else if (sparsity2 > 0.1) {
          sp1.multiply(sp2.toDense)
        }
        else {
          LocalMatrix.multiplySparseSparse(sp1, sp2)
        }
      case _ => throw new SparkException(s"Unsupported matrix type ${mat1.getClass.getName}")
    }
  }

  def frobenius(mat: MLMatrix): Double = {
    mat match {
      case ds: DenseMatrix =>
        var norm = 0.0
        for (elem <- ds.values)
          norm += elem * elem
        math.sqrt(norm)
      case sp: SparseMatrix =>
        var norm = 0.0
        for (elem <- sp.values)
          norm += elem * elem
        math.sqrt(norm)
    }
  }

  def matrixPow(mat: MLMatrix, p: Double): MLMatrix = {
    mat match {
      case ds: DenseMatrix =>
        val arr = ds.values.clone()
        for (i <- 0 until arr.length) {
          arr(i) = math.pow(ds.values(i), p)
        }
        new DenseMatrix(ds.numRows, ds.numCols, arr, ds.isTransposed)
      case sp: SparseMatrix =>
        val arr = sp.values.clone()
        for (i <- 0 until arr.length) {
          arr(i) = math.pow(sp.values(i), p)
        }
        new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, arr, sp.isTransposed)
    }
  }

  def SparkMatrixMultScalar(mat: SparkMatrix, a: Double): SparkMatrix = {
    mat match {
      case den: SparkDense =>
        val mvalues = den.values.clone()
        for (i <- 0 until mvalues.length) {
          mvalues(i) = mvalues(i) * a
        }
        new SparkDense(den.numRows, den.numCols, mvalues, den.isTransposed)
      case sp: SparkSparse =>
        val mvalues = sp.values.clone()
        for (i <- 0 until mvalues.length) {
          mvalues(i) = mvalues(i) * a
        }
        new SparkSparse(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, mvalues, sp.isTransposed)
    }
  }

  def addScalar(mat: MLMatrix, alpha: Double): MLMatrix = {
    mat match {
      case ds: DenseMatrix =>
        val arr = ds.values.clone()
        for (i <- 0 until arr.length) {
          arr(i) = ds.values(i) + alpha
        }
        new DenseMatrix(ds.numRows, ds.numCols, arr, ds.isTransposed)
      case sp: SparseMatrix =>
        val arr = sp.values.clone()
        for (i <- 0 until arr.length) {
          arr(i) = sp.values(i) + alpha
        }
        new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, arr, sp.isTransposed)
    }
  }

  def matrixDivideVector(mat: MLMatrix, vec: MLMatrix): MLMatrix = {
    // we know that vec is a dense matrix according to the application
    // mat can be dense or sparse
    require(mat.numCols == vec.numRows, s"Dimension error for matrix divide vector, " +
      s"mat.numCols=${mat.numCols}, vec.numRows=${vec.numRows}")
    mat match {
      case dm: DenseMatrix =>
        val arr = dm.values.clone()
        val div = vec.toArray
        val n = dm.numRows
        for (i <- 0 until arr.length) {
          if (div(i/n) != 0) { // avoid dividing by zero
            arr(i) = arr(i) / div(i/n)
          }
          else {
            arr(i) = 0.0
          }
        }
        new DenseMatrix(mat.numRows, mat.numCols, arr)
      case sp: SparseMatrix =>
        val arr = sp.values.clone()
        val div = vec.toArray
        val rowIdx = sp.rowIndices
        val colPtr = sp.colPtrs
        for (cid <- 0 until colPtr.length - 1) {
          val count = colPtr(cid + 1) - colPtr(cid)
          for (i <- 0 until count) {
            val idx = colPtr(cid) + i
            if (div(cid) != 0) { // avoid dividing by zero
              arr(idx) = arr(idx) / div(cid)
            }
            else {
              arr(idx) = 0.0
            }
          }
        }
        new SparseMatrix(sp.numRows, sp.numCols, colPtr, rowIdx, arr)
      case _ => throw new SparkException("Local matrix format not recognized!")
    }
  }

  def matrixEquals(mat: MLMatrix, v: Double): MLMatrix = {
    mat match {
      case den: DenseMatrix =>
        val values = den.toArray
        var nnz = 0L
        for (i <- 0 until values.length) {
          if (math.abs(v - values(i)) < 1e-6) {
            values(i) = 1
            nnz += 1
          }
          else {
            values(i) = 0
          }
        }
        // println(values.mkString("[", ",", "]"))
        if (nnz > 0.1 * den.numRows * den.numCols) {
          new DenseMatrix(mat.numRows, mat.numCols, values)
        }
        else {
          new DenseMatrix(mat.numRows, mat.numCols, values).toSparse
        }
      case sp: SparseMatrix =>
        val values = sp.toArray
        var nnz = 0L
        for (i <- 0 until values.length) {
          if (math.abs(v - values(i)) < 1e-6) {
            values(i) = 1
            nnz += 1
          }
          else {
            values(i) = 0
          }
        }
        if (nnz > 0.1 * sp.numRows * sp.numCols) {
          new DenseMatrix(mat.numRows, mat.numCols, values)
        }
        else {
          new DenseMatrix(mat.numRows, mat.numCols, values).toSparse
        }
    }
  }

  // return a uniform random local matrix
  def rand(nrows: Int, ncols: Int, min: Double, max: Double): MLMatrix = {
    val arr = new Array[Double](nrows * ncols)
    val random = scala.util.Random
    for (i <- 0 until arr.length) {
      arr(i) = min + (max - min) * random.nextDouble()
    }
    new DenseMatrix(nrows, ncols, arr)
  }

  def rankOneAdd(mat1: MLMatrix, mat2: MLMatrix, mat3: MLMatrix): MLMatrix = {
    val arr1 = Array.fill(mat1.numRows * mat1.numCols)(0.0)
    val arr = Array.fill(arr1.length)(0.0)
    for (i <- 0 until mat1.numRows) {
      for (j <- 0 until mat1.numCols) {
        val k = mat1.index(i, j)
        if (k >= 0) {
          arr(k) = arr1(k) + mat2(i, 0) * mat3(j, 0)
        } else {
          if (!mat1.isTransposed) {
            arr(i + mat1.numRows * j) = mat2(i, 0) * mat3(j, 0)
          } else {
            arr(j + mat1.numCols * i) = mat2(i, 0) * mat3(j, 0)
          }
        }
      }
    }
    new DenseMatrix(mat1.numRows, mat1.numCols, arr)
  }

  def max(mat1: MLMatrix, mat2: MLMatrix): MLMatrix = {
    require(mat1.numRows == mat2.numRows, s"#rows_mat1=${mat1.numRows},#rows_mat2=${mat2.numRows}")
    require(mat1.numCols == mat2.numCols, s"#cols_mat1=${mat1.numCols},#cols_mat2=${mat2.numCols}")
    val v1 = mat1.toArray
    val v2 = mat2.toArray
    val v = Array.fill[Double](v1.length)(0.0)
    for (i <- 0 until v1.length) {
      v(i) = math.max(v1(i), v2(i))
    }
    new DenseMatrix(mat1.numRows, mat1.numCols, v)
  }

  def min(mat1: MLMatrix, mat2: MLMatrix): MLMatrix = {
    require(mat1.numRows == mat2.numRows, s"#rows_mat1=${mat1.numRows},#rows_mat2=${mat2.numRows}")
    require(mat1.numCols == mat2.numCols, s"#cols_mat1=${mat1.numCols},#cols_mat2=${mat2.numCols}")
    val v1 = mat1.toArray
    val v2 = mat2.toArray
    val v = Array.fill[Double](v1.length)(0.0)
    for (i <- 0 until v1.length) {
      v(i) = math.min(v1(i), v2(i))
    }
    new DenseMatrix(mat1.numRows, mat1.numCols, v)
  }

  def UDF_Element_Matrix(x: Double, mat: MLMatrix, udf: (Double, Double) => Double): MLMatrix = {
    val rand = new scala.util.Random()
    if (math.abs(udf(rand.nextDouble(), 0.0) - 0.0) < 1e-6) { // preserving sparsity of the input
      mat match {
        case den: DenseMatrix =>
          val v = den.values
          for (i <- 0 until v.length) {
            v(i) = udf(x, v(i))
          }
          new DenseMatrix(den.numRows, den.numCols, v, den.isTransposed)
        case sp: SparseMatrix =>
          val v = sp.values
          for (i <- 0 until v.length) {
            v(i) = udf(x, v(i))
          }
          new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, v, sp.isTransposed)
        case _ => throw new SparkException("Illegal matrix type")
      }
    } else { // not preserving sparsity of the input
      val arr = mat.toArray
      val v = Array.fill[Double](arr.length)(0.0)
      var nnz = 0
      for (i <- 0 until v.length) {
        v(i) = udf(x, arr(i))
        if (v(i) != 0) nnz += 1
      }
      if (nnz > 0.5 * mat.numRows * mat.numCols) {
        new DenseMatrix(mat.numRows, mat.numCols, v)
      } else {
        new DenseMatrix(mat.numRows, mat.numCols, v).toSparse
      }
    }
  }

  // ind = -1; match the cell values
  // ind > 0, match the index and merge the cell values
  def UDF_Element_Match(ind: Long, cell: Double, mat: MLMatrix,
                        udf: (Double, Double) => Double): (Boolean, MLMatrix) = {
    val rand = new scala.util.Random()
    if (math.abs(udf(rand.nextDouble(), 0.0) - 0.0) < 1e-6) {
      mat match {
        case den: DenseMatrix =>
          val bloom = den.bloomFilter
          // bloom filter to skip the given mat
          if (ind < 0 && !bloom.contains(cell)) {
            (false, null)
          } else if (ind > 0 && !bloom.contains(ind)) {
            (false, null)
          } else {
            val filteredValues = Array.fill[Double](den.values.length)(0.0)
            var nnz = 0
            for (i <- 0 until den.values.length) {
              if (ind < 0) { // match values
                if (math.abs(cell - den.values(i)) < 1e-6) {
                  filteredValues(i) = udf(cell, den.values(i))
                  nnz += 1
                }
              } else { // match indices
                if (math.abs(ind - den.values(i)) < 1e-6) {
                  filteredValues(i) = udf(cell, den.values(i))
                  nnz += 1
                }
              }
            }
            if (nnz == 0) {
              (false, null)
            } else if (nnz > 0.5 * den.numRows * den.numCols) {
              (true, new DenseMatrix(den.numRows, den.numCols, filteredValues, den.isTransposed))
            } else {
              (true, new DenseMatrix(den.numRows, den.numCols,
                filteredValues, den.isTransposed).toSparse)
            }
          }
        case sp: SparseMatrix =>
          val bloom = sp.bloomFilter
          if (ind < 0 && !bloom.contains(cell)) {
            (false, null)
          } else if (ind > 0 && !bloom.contains(ind)) {
            (false, null)
          } else {
            val filteredValues = ArrayBuffer.empty[Double]
            val filteredRowIndices = ArrayBuffer.empty[Int]
            val filteredColPtrs = Array.fill[Int](sp.colPtrs.length)(0)
            var total = 0
            for (i <- 0 until sp.values.length) {
              if (ind < 0) {
                if (Math.abs(cell - sp.values(i)) <= 1e-6) {
                  total += 1
                  filteredValues += udf(cell, sp.values(i))
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
              } else {
                if (Math.abs(ind - sp.values(i)) <= 1e-6) {
                  total += 1
                  filteredValues += udf(cell, sp.values(i))
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
            }
            filteredColPtrs(sp.colPtrs.length - 1) = total
            for (k <- 1 until filteredColPtrs.length - 1) {
              filteredColPtrs(k) += filteredColPtrs(k - 1)
            }
            (true, new SparseMatrix(sp.numRows, sp.numCols, filteredColPtrs,
              filteredRowIndices.toArray, filteredValues.toArray, sp.isTransposed))
          }
        case _ => throw new SparkException("Illegal matrix type")
      }
    } else {
      val bloom = mat.bloomFilter
      if (ind < 0 && !bloom.contains(cell)) {
        (false, null)
      } else if (ind > 0 && !bloom.contains(ind)) {
        (false, null)
      } else {
        match_matrix_cells(ind, cell, mat, udf)
      }
    }
  }

   def match_matrix_cells(ind: Long, cell: Double, mat: MLMatrix,
                          udf: (Double, Double) => Double): (Boolean, MLMatrix) = {
    val arr = mat.toArray
    val v = Array.fill[Double](arr.length)(0.0)
    var nnz = 0
    for (i <- 0 until v.length) {
      if (ind < 0) { // match cell values
        if (math.abs(cell - arr(i)) < 1e-6) {
          v(i) = udf(cell, arr(i))
        }
        if (v(i) != 0) nnz += 1
      } else { // match indices
        if (math.abs(ind - arr(i)) < 1e-6) {
          v(i) = udf(cell, arr(i))
        }
        if (v(i) != 0) nnz += 1
      }
    }

    if (nnz == 0) {
      (false, null)
    } else if (nnz > 0.5 * mat.numRows * mat.numCols) {
      (true, new DenseMatrix(mat.numRows, mat.numCols, v))
    } else {
      (true, new DenseMatrix(mat.numRows, mat.numCols, v).toSparse)
    }
  }

  // idx < 0; match cell value with index1
  // idx > 0; match idx with index1
  def UDF_Value_Match_Index1(idx: Long, cell: Double, rid: Int, mat: MLMatrix, blkSize: Int,
                             udf: (Double, Double) => Double): (Boolean, MLMatrix) = {
    val offsetD1: Long = rid * blkSize
    // At most one row of B is matched with the cell
    if (idx < 0 && (cell < offsetD1 + 1 || cell > offsetD1 + mat.numRows)) {
      (false, null)
    } else if (idx > 0 && (idx < offsetD1 + 1 || idx > offsetD1 + mat.numRows)) {
      (false, null)
    } else {
      val rand = new scala.util.Random()
      if (math.abs(udf(rand.nextDouble(), 0.0) - 0.0) < 1e-6) {
        mat match {
          case den: DenseMatrix =>
            val v = Array.fill[Double](den.values.length)(0.0)
            for (i <- 0 until den.numRows) {
              for (j <- 0 until den.numCols) {
                if (idx < 0) {
                  if (math.abs(den(i, j) - 0.0) > 1e-6 &&
                    math.abs(cell - offsetD1 - i - 1) < 1e-6) {
                    v(j * den.numRows + i) = udf(cell, den(i, j))
                  }
                } else {
                  if (math.abs(den(i, j) - 0.0) > 1e-6 &&
                  math.abs(idx - offsetD1 - i - 1) < 1e-6) {
                    v(j * den.numRows + i) = udf(cell, den(i, j))
                  }
                }
              }
            }
            (true, new DenseMatrix(den.numRows, den.numCols, v).toSparse)
          case sp: SparseMatrix =>
            if (!sp.isTransposed) { // CSC format
              val matchValues = ArrayBuffer.empty[Double]
              val matchRowIndices = ArrayBuffer.empty[Int]
              val matchColCapacity = Array.fill[Int](sp.colPtrs.length)(0)
              val matchColPtrs = Array.fill[Int](sp.colPtrs.length)(0)
              for (j <- 0 until sp.numCols) {
                for (k <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                  val ind = sp.colPtrs(j) + k
                  val rowInd = sp.rowIndices(ind)
                  if (idx < 0) {
                    if (math.abs(cell - offsetD1 - rowInd - 1) < 1e-6) {
                      matchRowIndices += rowInd
                      matchValues += udf(cell, sp.values(ind))
                      matchColCapacity(j) += 1
                    }
                  } else {
                    if (math.abs(idx - offsetD1 - rowInd - 1) < 1e-6) {
                      matchRowIndices += rowInd
                      matchValues += udf(cell, sp.values(ind))
                      matchColCapacity(j) += 1
                    }
                  }
                }
              }
              for (j <- 1 until matchColPtrs.length) {
                matchColPtrs(j) = matchColPtrs(j - 1) + matchColCapacity(j - 1)
              }
              // matchColPtrs(matchColPtrs.length - 1) = matchValues.length
              (true, new SparseMatrix(sp.numRows, sp.numCols,
                matchColPtrs, matchRowIndices.toArray, matchValues.toArray, sp.isTransposed))
            } else { // CSR format
              val matchValues = ArrayBuffer.empty[Double]
              val matchColIndices = ArrayBuffer.empty[Int]
              val matchRowCapacity = Array.fill[Int](sp.colPtrs.length)(0)
              val matchRowPtrs = Array.fill[Int](sp.colPtrs.length)(0)
              for (i <- 0 until sp.numRows) {
                if (idx < 0) {
                  if (math.abs(cell - offsetD1 - i - 1) < 1e-6) {
                    for (k <- 0 until sp.colPtrs(i + 1) - sp.colPtrs(i)) {
                      val ind = sp.colPtrs(i) + k
                      matchColIndices += sp.rowIndices(ind)
                      matchValues += udf(cell, sp.values(ind))
                      matchRowCapacity(i) += 1
                    }
                  }
                } else {
                  if (math.abs(idx - offsetD1 - i - 1) < 1e-6) {
                    for (k <- 0 until sp.colPtrs(i + 1) - sp.colPtrs(i)) {
                      val ind = sp.colPtrs(i) + k
                      matchColIndices += sp.rowIndices(ind)
                      matchValues += udf(cell, sp.values(ind))
                      matchRowCapacity(i) += 1
                    }
                  }
                }
              }
              for (i <- 1 until matchRowPtrs.length) {
                matchRowPtrs(i) = matchRowPtrs(i - 1) + matchRowCapacity(i - 1)
              }
              // matchRowPtrs(matchRowPtrs.length - 1) = matchValues.length
              (true, new SparseMatrix(sp.numRows, sp.numCols,
                matchRowPtrs, matchColIndices.toArray, matchValues.toArray, sp.isTransposed))
            }
          case _ => throw new SparkException("Illegal matrix type")
        }
      } else {
        val arr = mat.toArray
        val v = Array.fill[Double](arr.length)(0.0)
        for (i <- 0 until mat.numRows) {
          for (j <- 0 until mat.numCols) {
            if (idx < 0) {
              if (math.abs(cell - offsetD1 - i - 1) < 1e-6) {
                v(j * mat.numRows + i) = udf(cell, mat(i, j))
              }
            } else {
              if (math.abs(idx - offsetD1 - i - 1) < 1e-6) {
                v(j * mat.numRows + i) = udf(cell, mat(i, j))
              }
            }
          }
        }
        (true, new DenseMatrix(mat.numRows, mat.numCols, v).toSparse)
      }
    }
  }

  // idx < 0; match cell value with index2
  // idx > 0; match idx with index2
  def UDF_Value_Match_Index2(idx: Long, cell: Double, cid: Int, mat: MLMatrix, blkSize: Int,
                             udf: (Double, Double) => Double): (Boolean, MLMatrix) = {
    val offsetD2: Long = cid * blkSize
    // At most one column of B is matched with the cell
    if (idx < 0 && (cell < offsetD2 + 1 || cell > offsetD2 + mat.numCols)) {
      (false, null)
    } else if (idx > 0 && (idx < offsetD2 + 1 || idx > offsetD2 + mat.numCols)) {
      (false, null)
    } else {
      val rand = new scala.util.Random()
      if (math.abs(udf(rand.nextDouble(), 0.0) - 0.0) < 1e-6) {
        mat match {
          case den: DenseMatrix =>
            val v = Array.fill[Double](den.values.length)(0.0)
            for (i <- 0 until den.numRows) {
              for (j <- 0 until den.numCols) {
                if (idx < 0) {
                  if (math.abs(den(i, j) - 0.0) > 1e-6 &&
                    math.abs(cell - offsetD2 - j - 1) < 1e-6) {
                    v(j * den.numRows + i) = udf(cell, den(i, j))
                  }
                } else {
                  if (math.abs(den(i, j) - 0.0) > 1e-6 &&
                    math.abs(idx - offsetD2 - j - 1) < 1e-6) {
                    v(j * den.numRows + i) = udf(cell, den(i, j))
                  }
                }
              }
            }
            (true, new DenseMatrix(den.numRows, den.numCols, v).toSparse)
          case sp: SparseMatrix =>
            if (!sp.isTransposed) { // CSC format
              val matchValues = ArrayBuffer.empty[Double]
              val matchRowIndices = ArrayBuffer.empty[Int]
              val matchColCapacity = Array.fill[Int](sp.colPtrs.length)(0)
              val matchColPtrs = Array.fill[Int](sp.colPtrs.length)(0)
              for (j <- 0 until sp.numCols) {
                if (idx < 0) {
                  if (math.abs(cell - offsetD2 - j - 1) < 1e-6) {
                    for (k <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                      val ind = sp.colPtrs(j) + k
                      matchRowIndices += sp.rowIndices(ind)
                      matchValues += udf(cell, sp.values(ind))
                      matchColCapacity(j) += 1
                    }
                  }
                } else {
                  if (math.abs(idx - offsetD2 - j - 1) < 1e-6) {
                    for (k <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                      val ind = sp.colPtrs(j) + k
                      matchRowIndices += sp.rowIndices(ind)
                      matchValues += udf(cell, sp.values(ind))
                      matchColCapacity(j) += 1
                    }
                  }
                }
              }
              for (j <- 1 until matchColPtrs.length) {
                matchColPtrs(j) = matchColPtrs(j - 1) + matchColCapacity(j - 1)
              }
              // matchColPtrs(matchColPtrs.length - 1) = matchValues.length
              (true, new SparseMatrix(sp.numRows, sp.numCols,
                matchColPtrs, matchRowIndices.toArray, matchValues.toArray, sp.isTransposed))
            } else { // CSR format
              val matchValues = ArrayBuffer.empty[Double]
              val matchColIndices = ArrayBuffer.empty[Int]
              val matchRowCapacity = Array.fill[Int](sp.colPtrs.length)(0)
              val matchRowPtrs = Array.fill[Int](sp.colPtrs.length)(0)
              for (i <- 0 until sp.numRows) {
                for (k <- 0 until sp.colPtrs(i + 1) - sp.colPtrs(i)) {
                  val ind = sp.colPtrs(i) + k
                  val colInd = sp.rowIndices(ind)
                  if (idx < 0) {
                    if (math.abs(cell - offsetD2 - colInd - 1) < 1e-6) {
                      matchColIndices += colInd
                      matchValues += udf(cell, sp.values(ind))
                      matchRowCapacity(i) += 1
                    }
                  } else {
                    if (math.abs(idx - offsetD2 - colInd - 1) < 1e-6) {
                      matchColIndices += colInd
                      matchValues += udf(cell, sp.values(ind))
                      matchRowCapacity(i) += 1
                    }
                  }
                }
              }
              for (i <- 1 until matchRowPtrs.length) {
                matchRowPtrs(i) = matchRowPtrs(i - 1) + matchRowCapacity(i - 1)
              }
              // matchRowPtrs(matchRowPtrs.length - 1) = matchValues.length
              (true, new SparseMatrix(sp.numRows, sp.numCols,
                matchRowPtrs, matchColIndices.toArray, matchValues.toArray, sp.isTransposed))
            }
          case _ => throw new SparkException("Illegal matrix type")
        }
      } else {
        val arr = mat.toArray
        val v = Array.fill[Double](arr.length)(0.0)
        for (i <- 0 until mat.numRows) {
          for (j <- 0 until mat.numCols) {
            if (idx < 0) {
              if (math.abs(cell - offsetD2 - j - 1) < 1e-6) {
                v(j * mat.numRows + i) = udf(cell, mat(i, j))
              }
            } else {
              if (math.abs(idx - offsetD2 - j - 1) < 1e-6) {
                v(j * mat.numRows + i) = udf(cell, mat(i, j))
              }
            }
          }
        }
        (true, new DenseMatrix(mat.numRows, mat.numCols, v).toSparse)
      }
    }
  }

  // To do rid1 = rid2 join, mat1's rid1 = mat2's rid2
  def joinRidRidBlocks(rid1: Int, cid1: Int, mat1: MLMatrix,
                       rid2: Int, cid2: Int, mat2: MLMatrix,
                       udf: (Double, Double) => Double): ArrayBuffer[((Int, Int, Int, Int), MLMatrix)] = {

    require(rid1 == rid2, s"rid1 must be equal to rid2, rid1=$rid1, rid2=$rid2")
    val tensor = new ArrayBuffer[((Int, Int, Int, Int), MLMatrix)]()
    var kIndx = 0
    for (j <- 0 until mat1.numCols) {
      // duplicate jth column everywhere in a tmp matrix
      val key = (rid1, cid1, cid2, kIndx)
      mat1 match {
        case den: DenseMatrix =>
          val arr = new Array[Double](den.values.length)
          if (!den.isTransposed) {
            for (i <- 0 until arr.length) {
              arr(i) = den.values((i % den.numRows) + kIndx * den.numRows)
            }
          } else {
            for (i <- 0 until arr.length) {
              arr(i) = den.values((i % den.numCols) * den.numCols + kIndx)
            }
          }
          tensor.append((key, LocalMatrix.compute(new DenseMatrix(den.numRows, den.numCols, arr), mat2, udf)))
        case sp: SparseMatrix =>
          if (!sp.isTransposed) {
            val cnt = sp.colPtrs(kIndx + 1) - sp.colPtrs(kIndx)
            val arr = new Array[Double](cnt * sp.numCols)
            val rowIndx = new Array[Int](arr.length)
            val colPtrs = new Array[Int](sp.numCols + 1)
            for (i <- 1 until colPtrs.length) {
              colPtrs(i) = colPtrs(i - 1) + cnt
            }
            for (i <- 0 until arr.length) {
              val idx = sp.colPtrs(kIndx) + (i % cnt)
              arr(i) = sp.values(idx)
              rowIndx(i) = sp.rowIndices(idx)
            }
            tensor.append((key, LocalMatrix.compute(new SparseMatrix(sp.numRows, sp.numCols, colPtrs, rowIndx, arr), mat2, udf)))
          } else {
            // look for kIndx-th column
            var cnt = 0
            val currVal = new ArrayBuffer[Double]()
            val currRowIdx = new ArrayBuffer[Int]()
            for (i <- 0 until sp.numRows) {
              for (k <- 0 until sp.colPtrs(i + 1) - sp.colPtrs(i)) {
                val ind = sp.colPtrs(i) + k
                if (sp.rowIndices(ind) == kIndx) {
                  cnt += 1
                  currVal.append(sp.values(ind))
                  currRowIdx.append(i)
                }
              }
            }
            // copy the kIndx-th column multiple times
            val arr = new Array[Double](cnt * sp.numCols)
            val rowIndx = new Array[Int](arr.length)
            val colPtrs = new Array[Int](sp.numCols + 1)
            for (i <- 1 until colPtrs.length) {
              colPtrs(i) = colPtrs(i - 1) + cnt
            }
            for (i <- 0 until arr.length) {
              val idx = i % cnt
              arr(i) = currVal(idx)
              rowIndx(i) = currRowIdx(idx)
            }
            tensor.append((key, LocalMatrix.compute(new SparseMatrix(sp.numRows, sp.numCols, colPtrs, rowIndx, arr), mat2, udf)))
          }
        case _ => throw new SparkException("matrix type not supported")
      }
      kIndx += 1
    }
    tensor
  }

  // To do cid1 = rid2 join, mat1's cid1 = mat2's rid2
  def joinCidRidBlocks(rid1: Int, cid1: Int, mat1: MLMatrix,
                       rid2: Int, cid2: Int, mat2: MLMatrix,
                       udf: (Double, Double) => Double): ArrayBuffer[((Int, Int, Int, Int), MLMatrix)] = {
    require(cid1 == rid2, s"cid1 must be equal to rid2, cid1=$cid1, rid2=$rid2")
    joinRidRidBlocks(cid1, rid1, mat1.transpose, rid2, cid2, mat2, udf)
  }

  // To do cid1 = cid2 join, mat1's cid1 = mat2's cid2
  def joinCidCidBlocks(rid1: Int, cid1: Int, mat1: MLMatrix,
                       rid2: Int, cid2: Int, mat2: MLMatrix,
                       udf: (Double, Double) => Double): ArrayBuffer[((Int, Int, Int, Int), MLMatrix)] = {
    require(cid1 == cid2, s"cid1 must be equal to cid2, cid1=$cid1, cid2=$cid2")
    val tensor = new ArrayBuffer[((Int, Int, Int, Int), MLMatrix)]()
    var kIndx = 0
    for (i <- 0 until mat1.numRows) {
      // duplicate ith row everywhere in a tmp matrix
      val key = (rid1, cid1, rid2, kIndx)
      mat1 match {
        case den: DenseMatrix =>
          val arr = new Array[Double](den.values.length)
          if (!den.isTransposed) {
            for (j <- 0 until arr.length) {
              arr(j) = den.values((j % den.numRows) * den.numRows + kIndx)
            }
          } else {
            for (j <- 0 until arr.length) {
              arr(j) = den.values((j % den.numCols) + kIndx * den.numCols)
            }
          }
          tensor.append((key, LocalMatrix.compute(new DenseMatrix(den.numRows, den.numCols, arr), mat2, udf)))
        case sp: SparseMatrix =>
          if (!sp.isTransposed) {
            // look for kIndx-th row
            var cnt = 0
            val currVal = new ArrayBuffer[Double]()
            val currColIdx = new ArrayBuffer[Int]()
            for (j <- 0 until sp.numCols) {
              for (k <- 0 until sp.colPtrs(j + 1) - sp.colPtrs(j)) {
                val ind = sp.colPtrs(j) + k
                if (sp.rowIndices(ind) == kIndx) {
                  cnt += 1
                  currVal.append(sp.values(ind))
                  currColIdx.append((j))
                }
              }
            }
            // copy the kIndx-th row multiple times
            val arr = new Array[Double](cnt * sp.numRows)
            val colIndx = new Array[Int](arr.length)
            val rowPtrs = new Array[Int](sp.numRows + 1)
            for (j <- 1 until rowPtrs.length) {
              rowPtrs(j) = rowPtrs(j - 1) + cnt
            }
            for (j <- 0 until arr.length) {
              val idx = j % cnt
              arr(j) = currVal(idx)
              colIndx(j) = currColIdx(idx)
            }
            tensor.append((key, LocalMatrix.compute(new SparseMatrix(sp.numRows, sp.numCols, rowPtrs, colIndx, arr, true), mat2, udf)))
          } else {
            val cnt = sp.colPtrs(kIndx + 1) - sp.colPtrs(kIndx)
            val arr = new Array[Double](cnt * sp.numRows)
            val colIndx = new Array[Int](arr.length)
            val rowPtrs = new Array[Int](sp.numRows + 1)
            for (j <- 1 until rowPtrs.length) {
              rowPtrs(j) = rowPtrs(j - 1) + cnt
            }
            for (j <- 0 until arr.length) {
              val idx = sp.colPtrs(kIndx) + (j % cnt)
              arr(j) = sp.values(idx)
              colIndx(j) = sp.rowIndices(idx)
            }
            tensor.append((key, LocalMatrix.compute(new SparseMatrix(sp.numRows, sp.numCols, rowPtrs, colIndx, arr, true), mat2, udf)))
          }
        case _ => throw new SparkException("matrix type not found")
      }
      kIndx += 1
    }
    tensor
  }

  // To do rid1 = cid2 join, mat1's rid1 = mat2's cid2
  def joinRidCidBlocks(rid1: Int, cid1: Int, mat1: MLMatrix,
                       rid2: Int, cid2: Int, mat2: MLMatrix,
                       udf: (Double, Double) => Double): ArrayBuffer[((Int, Int, Int, Int), MLMatrix)] = {
    require(rid1 == cid2, s"rid1 must be equal to cid2, rid1=$rid1, cid2=$cid2")
    joinCidCidBlocks(cid1, rid1, mat1.transpose, rid2, cid2, mat2, udf)
  }
}

object TestSparse {
  def main (args: Array[String]) {
    val va = Array[Double](1, 2, 3, 4, 5, 6)
    val rowIdxa = Array[Int](0, 2, 1, 0, 1, 2)
    val colPtra = Array[Int](0, 2, 3, 6)
    val vb = Array[Double](3, 1, 2, 2)
    val rowIdxb = Array[Int](1, 0, 2, 0)
    val colPtrb = Array[Int](0, 1, 3, 4)
    val spmat1 = new SparseMatrix(3, 3, colPtra, rowIdxa, va)
    val spmat2 = new SparseMatrix(3, 3, colPtrb, rowIdxb, vb)
    // scalastyle:off
    println(spmat1)
    println("-" * 20)
    println(spmat2)
    println("-" * 20)
    println(LocalMatrix.multiplySparseSparse(spmat1, spmat2))
    println("-" * 20)
    println(LocalMatrix.elementWiseMultiply(spmat1, spmat2))
    println("-" * 20)
    println(LocalMatrix.add(spmat1, spmat2))
    println("-" * 20)
    // scalastyle:on
    // println(LocalMatrix.addSparseSparseNative(spmat1, spmat2))
    val vd1 = Array[Double](1, 4, 7, 2, 5, 8, 3, 6, 9)
    val vd2 = Array[Double](1, 2, 3, 1, 2, 3, 1, 2, 3)
    val den1 = new DenseMatrix(3, 3, vd1)
    val den2 = new DenseMatrix(3, 3, vd2)
    val denC = DenseMatrix.zeros(3, 3)
    val denV = new DenseMatrix(3, 1, Array[Double](1, 2, 3))
    LocalMatrix.incrementalMultiply(spmat2, spmat2.transpose, denC)
    // scalastyle:off
    println(denC)
    println("-" * 20)
    println(LocalMatrix.multiplySparseMatDenseVec(spmat1, denV))
    println("-" * 20)
    println(LocalMatrix.multiplySparseSparse(spmat1, spmat2.transpose))
    println(spmat1)
    println(LocalMatrix.matrixDivideVector(den2, denV))
    // scalastyle:on
  }
}