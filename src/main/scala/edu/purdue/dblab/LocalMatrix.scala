package edu.purdue.dblab

import org.apache.spark.SparkException
import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by yongyangyu on 7/19/15.
 * Expose some common operations on MLMatrices.
 * Especially, provide matrix multiplication implementation for CSC format sparse matrices.
 */
object LocalMatrix {
    def add(a: MLMatrix, b: MLMatrix): MLMatrix = {
        if (a != null && b != null) {
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
        else {
            if (a != null && b == null) a
            else if (a == null && b != null) b
            else null
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
        new DenseMatrix(ma.numRows, ma.numCols, arr)
    }

    private def multiplyScalarSparse(alpha: Double, ma: SparseMatrix): MLMatrix = {
        val arr = for (elem <- ma.values) yield alpha * elem
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

    def elementWiseMultiply(mat1: MLMatrix, mat2: MLMatrix): MLMatrix = {
        require(mat1.numRows == mat2.numRows, s"mat1.numRows = ${mat1.numRows}, mat2.numRows = ${mat2.numRows}")
        require(mat1.numCols == mat2.numCols, s"mat1.numCols = ${mat1.numCols}, mat2.numCols = ${mat2.numCols}")
        (mat1, mat2) match {
            case (ma: DenseMatrix, mb: DenseMatrix) => elementWiseOpDenseDense(ma, mb)
            case (ma: DenseMatrix, mb: SparseMatrix) => elementWiseOpDenseSparse(ma, mb)
            case (ma: SparseMatrix, mb: DenseMatrix) => elementWiseOpDenseSparse(mb, ma)
            case (ma: SparseMatrix, mb: SparseMatrix) => elementWiseOpSparseSparse(ma, mb)
        }
    }

    def elementWiseDivide(mat1: MLMatrix, mat2: MLMatrix): MLMatrix = {
        require(mat1.numRows == mat2.numRows, s"mat1.numRows = ${mat1.numRows}, mat2.numRows = ${mat2.numRows}")
        require(mat1.numCols == mat2.numCols, s"mat1.numCols = ${mat1.numCols}, mat2.numCols = ${mat2.numCols}")
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

    def elementWiseOpSparseSparseNative(ma: SparseMatrix, mb: SparseMatrix, op: Int = 0): MLMatrix = {
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
    // cannot handle the case when C is a sparse matrix and holding a non-exsitent entry for the product
    def incrementalMultiply(A: MLMatrix, B: MLMatrix, C: MLMatrix): MLMatrix = {
        require(A.numRows == C.numCols && B.numCols == C.numCols, s"dimension mismatch " +
        s"A.numRows = ${A.numRows}, C.numRows = ${C.numRows}, B.numCols = ${B.numCols}, C.numCols = ${C.numCols}")
        (A, B, C) match {
            case (matA: DenseMatrix, matB: DenseMatrix, matC: DenseMatrix) => incrementalMultiplyDenseDense(matA, matB, matC)
            case (matA: DenseMatrix, matB: SparseMatrix, matC: DenseMatrix) => incrementalMultiplyDenseSparse(matA, matB, matC)
            case (matA: SparseMatrix, matB: DenseMatrix, matC: DenseMatrix) => incrementalMultiplySparseDense(matA, matB, matC)
            case (matA: SparseMatrix, matB: SparseMatrix, matC: DenseMatrix) => incrementalMultiplySparseSparseDense(matA, matB, matC)
            //case (matA: SparseMatrix, matB: SparseMatrix, matC: SparseMatrix) =>
            case _ => new SparkException(s"incrememtalMultiply does not apply to the required format of matrices, " +
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
            val count = colPtr(c+1) - colPtr(c)
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
            val count = rowPtr(r+1) - rowPtr(r)
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

    private def incrementalMultiplySparseSparseDense(A: SparseMatrix, B: SparseMatrix, C: DenseMatrix) = {
        if (!A.isTransposed && !B.isTransposed) { // both A and B are not transposed
            // process as column transformation on A
            val valuesb = B.values
            val rowIdxb = B.rowIndices
            val colPtrb = B.colPtrs
            val valuesa = A.values
            val rowIdxa = A.rowIndices
            val colPtra = A.colPtrs
            for (cb <- 0 until B.numCols) {
                val countb = colPtrb(cb+1) - colPtrb(cb)
                val startb = colPtrb(cb)
                for (ridxb <- startb until startb + countb) {
                    val vb = valuesb(ridxb)
                    val rowb = rowIdxb(ridxb)
                    // get rowb-th column from A and multiply it with vb
                    val counta = colPtra(rowb+1) - colPtra(rowb)
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
                val counta = rowPtra(ra+1) - rowPtra(ra)
                val starta = rowPtra(ra)
                for (cidxa <- starta until starta + counta) {
                    val va = valuesa(cidxa)
                    val cola = colIdxa(cidxa)
                    // get cola-th row from B and multiply it with va
                    val countb = rowPtrb(cola+1) - rowPtrb(cola)
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
        else if (A.isTransposed && !B.isTransposed) { // A is stored in row format and B is stored in column format
            // this is an easy (natrual) case
            val valuesa = A.values
            val colIdxa = A.rowIndices
            val rowPtra = A.colPtrs
            val valuesb = B.values
            val rowIdxb = B.rowIndices
            val colPtrb = B.colPtrs
            for (ra <- 0 until A.numRows) {
                val counta = rowPtra(ra+1) - rowPtra(ra)
                val starta = rowPtra(ra)
                val entrya: ArrayBuffer[(Int, Double)] = new ArrayBuffer[(Int, Double)]()
                for (cidxa <- starta until starta + counta) {
                    entrya.append((colIdxa(cidxa), valuesa(cidxa)))
                }
                for (cb <- 0 until B.numCols) {
                    val countb = colPtrb(cb+1) - colPtrb(cb)
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
                val counta = colPtra(idx+1) - colPtra(idx)
                val starta = colPtra(idx)
                val entrya: ArrayBuffer[(Int, Double)] = new ArrayBuffer[(Int, Double)]()
                for (ridx <- starta until starta + counta) {
                    entrya.append((rowIdxa(ridx), valuesa(ridx)))
                }
                val countb = rowPtrb(idx+1) - rowPtrb(idx)
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
        //println(LocalMatrix.addSparseSparseNative(spmat1, spmat2))
        val vd1 = Array[Double](1,4,7,2,5,8,3,6,9)
        val vd2 = Array[Double](1,2,3,1,2,3,1,2,3)
        val den1 = new DenseMatrix(3,3,vd1)
        val den2 = new DenseMatrix(3,3,vd2)
        val denC = DenseMatrix.zeros(3,3)
        val denV = new DenseMatrix(3, 1, Array[Double](1,2,3))
        LocalMatrix.incrementalMultiply(spmat2, spmat2.transpose, denC)
        println(denC)
        println("-" * 20)
        println(LocalMatrix.multiplySparseMatDenseVec(spmat1, denV))
        println("-" * 20)
        println(LocalMatrix.multiplySparseSparse(spmat1, spmat2.transpose))
    }
}
