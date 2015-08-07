package edu.purdue.dblab

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{Matrix => MLMatrix, SparseMatrix, DenseMatrix}
import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yongyangyu on 7/19/15.
 * Expose some common operations on MLMatrices.
 * Especially, provide matrix multiplication implementation for CSC format sparse matrices.
 */
object LocalMatrix {
    def add(a: MLMatrix, b: MLMatrix): MLMatrix = {
        require(a.numRows == b.numRows, s"Matrix A and B must have the same number of rows. But found " +
        s"A.numRows = ${a.numRows}, B.numRows = ${b.numRows}")
        require(a.numCols == b.numCols, s"Matrix A and B must have the same number of cols. But found " +
        s"A.numCols = ${a.numCols}, B.numCols = ${b.numCols}")
        (a, b) match {
          case (ma: DenseMatrix, mb: DenseMatrix) => addDense(ma, mb)
          case (ma: DenseMatrix, mb: SparseMatrix) => addDenseSparse(ma, mb)
          case (ma: SparseMatrix, mb: DenseMatrix) => addDenseSparse(mb, ma)
          case (ma: SparseMatrix, mb: SparseMatrix) => addSparseSparse(ma, mb)
        }
    }

    def addDense(ma: DenseMatrix, mb: DenseMatrix): MLMatrix = {
        val (arr1, arr2) = (ma.toArray, mb.toArray)
        val arr = Array.fill(arr1.length)(0.0)
        for (i <- 0 until arr.length) {
            arr(i) = arr1(i) + arr2(i)
        }
        new DenseMatrix(ma.numRows, ma.numCols, arr)
    }

    private def addDenseSparse(ma: DenseMatrix, mb: SparseMatrix): MLMatrix = {
        val (arr1, arr2) = (ma.toArray, mb.toArray)
        val arr = Array.fill(arr1.length)(0.0)
        for (i <- 0 until arr.length) {
            arr(i) = arr1(i) + arr2(i)
        }
        new DenseMatrix(ma.numRows, ma.numCols, arr)
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
    def addSparseSparseNative(ma: SparseMatrix, mb: SparseMatrix): MLMatrix = {
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
        var va = ma.values
        var rowIdxa = ma.rowIndices
        var colPtra = ma.colPtrs
        if (ma.isTransposed) {
            val tmp = CSR2SCS(ma)
            rowIdxa = tmp._1
            colPtra = tmp._2
            va = tmp._3
        }
        var vb = mb.values
        var rowIdxb = mb.rowIndices
        var colPtrb = mb.colPtrs
        val vc = ArrayBuffer[Double]()
        if (mb.isTransposed) {
            val tmp = CSR2SCS(mb)
            rowIdxb = tmp._1
            colPtrb = tmp._2
            vb = tmp._3
        }
        val rowIdxc = ArrayBuffer[Int]()
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
            colMap(colIdxcsr(i)) = elem   //update for the map
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
        throw new SparkException("binary search failed for CSR2CSC conversion!")
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
        val arr = for (elem <- ma.toArray) yield elem * alpha
        new DenseMatrix(ma.numRows, ma.numCols, arr)
    }

    private def multiplyScalarSparse(alpha: Double, ma: SparseMatrix): MLMatrix = {
        val arr = for (elem <- ma.values) yield  elem * alpha
        new SparseMatrix(ma.numRows, ma.numCols, ma.colPtrs, ma.rowIndices, arr)
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
            val breezeMatrix = new BSM[Double](mat.values, mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices)
            breezeMatrix.t
        }
    }

    def fromBreeze(breeze: BM[Double]): MLMatrix = {
        breeze match {
          case dm: BDM[Double] => new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
          case sp: BSM[Double] => new SparseMatrix(sp.rows, sp.cols, sp.colPtrs, sp.rowIndices, sp.data)
          case _ => throw new UnsupportedOperationException(s"Do not support conversion from type ${breeze.getClass.getName}")
        }
    }
}

object TestSparse {
    def main (args: Array[String]) {
        val va = Array[Double](1,2,3,4,5,6)
        val rowIdxa = Array[Int](0,2,1,0,1,2)
        val colPtra = Array[Int](0,2,3,6)
        val vb = Array[Double](3,1,2,2)
        val rowIdxb = Array[Int](1,0,2,0)
        val colPtrb = Array[Int](0,1,3,4)
        val spmat1 = new SparseMatrix(3,3,colPtra,rowIdxa,va)
        val spmat2 = new SparseMatrix(3,3,colPtrb,rowIdxb,vb)
        println(LocalMatrix.multiplySparseSparse(spmat1.transpose, spmat2))
        //println(LocalMatrix.addSparseSparseNative(spmat1, spmat2))

    }
}
