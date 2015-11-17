package edu.purdue.dblab.matrix

import org.apache.spark.mllib.linalg.{Matrix => MLMatrix, _}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yongyangyu on 6/28/15.
 * This companion object provides certain missing helpers for vector operations,
 * i.e., inner product, scalar multiplication, addition, etc.
 */
object LocalVector {
  def innerProdOfVectors(vec1: Vector, vec2: Vector): Double = {
    val s1 = vec1.size
    val s2 = vec2.size
    require(s1 == s2, s"two vectors need to be of the same size but found s1=$s1, s2=$s2")
    (vec1, vec2) match {
      case (v1: DenseVector, v2: DenseVector) => innerProdOfDenseDense(v1, v2)
      case (v1: SparseVector, v2: DenseVector) => innerProdOfSparseDense(v1, v2)
      case (v1: DenseVector, v2 : SparseVector) => innerProdOfSparseDense(v2, v1)
      case (v1: SparseVector, v2: SparseVector) => innerProdOfSparseSparse(v1, v2)
    }
  }

  private def innerProdOfDenseDense(vec1: DenseVector, vec2: DenseVector): Double = {
    var res = 0.0
    for (i <- 0 until vec1.size) {
          res += vec1(i) * vec2(i)
      }
    res
  }
  private def innerProdOfSparseDense(vec1: SparseVector, vec2: DenseVector): Double = {
      val idx = vec1.indices
      val values = vec1.values
      var res = 0.0
      for (i <- 0 until idx.length) {
          res += values(i) * vec2(idx(i))
      }
      res
  }
  private def innerProdOfSparseSparse(vec1: SparseVector, vec2: SparseVector): Double = {
      val spvec1 = vec1.toSparse
      val spvec2 = vec2.toSparse
      val idx1 = spvec1.indices
      val v1 = spvec1.values
      val idx2 = spvec2.indices
      val v2 = spvec2.values
      var inner = 0.0
      var (i, j) = (0, 0)
      while (i < idx1.length && j < idx2.length) {
        if (idx1(i) == idx2(j)) {
          inner += v1(i) * v2(j)
          i += 1
          j += 1
        }
        else if (idx1(i) < idx2(j)) {
          i += 1
        }
        else {
          j += 1
        }
      }
      inner
  }

  def multiplyScalar(alpha: Double, vec: Vector): Vector = {
    if (vec.isInstanceOf[DenseVector]) {
      val old = vec.asInstanceOf[DenseVector]
      val entries = old.toArray
      val newEntries = new Array[Double](entries.length)
      for (i <- 0 until entries.length) {
        newEntries(i) = alpha * entries(i)
      }
      Vectors.dense(newEntries)
    }
    else {
      val old = vec.asInstanceOf[SparseVector]
      val entries = old.values
      val newEntries = new Array[Double](entries.length)
      for (i <- 0 until entries.length) {
        newEntries(i) = alpha * entries(i)
      }
      Vectors.sparse(old.size, old.indices, newEntries)
    }
  }

  def add(vec1: Vector, vec2: Vector): Vector = {
      require(vec1.size == vec2.size, s"size of vec1: ${vec1.size} not equals to that of vec2: ${vec2.size}")
     (vec1, vec2) match {
       case (v1: DenseVector, v2: DenseVector) => addDenseDense(v1, v2)
       case (v1: SparseVector, v2: DenseVector) => addSparseDense(v1, v2)
       case (v1: DenseVector, v2: SparseVector) => addSparseDense(v2, v1)
       case (v1: SparseVector, v2: SparseVector) => addSparseSparse(v1, v2)
     }
  }

  private def addDenseDense(vec1: DenseVector, vec2: DenseVector): Vector = {
      val arr = Array.fill(vec1.size)(0.0)
      for (i <- 0 until vec1.size) {
        arr(i) = vec1(i) + vec2(i)
      }
      Vectors.dense(arr)
  }

  private def addSparseDense(vec1: SparseVector, vec2: DenseVector): Vector = {
      val arr = Array.fill(vec1.size)(0.0)
      val idx = vec1.indices
      val vs = vec1.values
      var j = 0
      for (i <- 0 until vec1.size) {
          if (j < idx.length && i == idx(j)) {
              arr(i) = vs(j) + vec2(i)
              j += 1
          }
          else {
              arr(i) = vec2(i)
          }
      }
      Vectors.dense(arr)
  }

  private def addSparseSparse(vec1: SparseVector, vec2: SparseVector): Vector = {
    val idx1 = vec1.indices
    val v1 = vec1.values
    val idx2 = vec2.indices
    val v2 = vec2.values
    val idx = new ArrayBuffer[Int]()
    val v = new ArrayBuffer[Double]()
    var (i, j) = (0, 0)
    while (i < idx1.length && j < idx2.length) {
      if (idx1(i) == idx2(j)) {
        idx += idx1(i)
        v += v1(i) + v2(j)
        i += 1
        j += 1
      }
      else if (idx1(i) < idx2(j)) {
        idx += idx1(i)
        v += v1(i)
        i += 1
      }
      else {
        idx += idx2(j)
        v += v2(j)
        j += 1
      }
    }
    while (i < idx1.length) {
      idx += idx1(i)
      v += v1(i)
      i += 1
    }
    while (j < idx2.length) {
      idx += idx2(j)
      v += v2(j)
      j += 1
    }
    Vectors.sparse(vec1.size, idx.toArray, v.toArray)
  }

  def outerProduct(vec1: Vector, vec2: Vector): MLMatrix = {
      val (rowSize, colSize) = (vec1.size, vec2.size)
      val arr1 = vec1.toArray
      val arr2 = vec2.toArray
      val arr = Array.fill(rowSize * colSize)(0.0)
      for (i <- 0 until colSize; j <- 0 until rowSize) {
          arr(i * rowSize + j) = vec1(j) * vec2(i)
      }
      new DenseMatrix(rowSize, colSize, arr)
  }

  /*
   *  Local vector is supposed to be a row vector and the multiplication with
   *  a column partitioned matrix. The result is also a row vector.
   */
  def multiplyColumnMatrix(vec: Vector, matrix: ColumnPartitionMatrix): Vector = {
      require(vec.size == matrix.nRows().toInt, s"Cannot perform multiply between a row vector and a column " +
        s"partition matrix, vec size = ${vec.size}, matrix row size = ${matrix.nRows()}")
      val bcast = matrix.cols.context.broadcast(vec.toArray)
      val res = matrix.cols.map { col =>
        val v = Vectors.dense(bcast.value)
        (col.cid, LocalVector.innerProdOfVectors(col.cvec, v))
      }.collect().sorted
      Vectors.dense(res.map(x => x._2))
  }

  def multiplyRowMatrix(vec: Vector, matrix: RowPartitionMatrix): Vector = {
      require(vec.size == matrix.nRows().toInt, s"Cannot perform multiply between a row vector and a row " +
      s"partition matrix, vec size = ${vec.size}, matrix row size = ${matrix.nRows()}")
      val bcast = matrix.rows.context.broadcast(vec.toArray)
      matrix.rows.map { row =>
          val rid = row.ridx
          LocalVector.multiplyScalar(bcast.value(rid.toInt), row.rvec)
      }.fold(Vectors.zeros(matrix.nCols().toInt))(LocalVector.add(_, _))
  }

  def main (args: Array[String]) {
      val v1 = Vectors.sparse(20, Array(5,10,15), Array(1,2,3))
      val v2 = Vectors.sparse(20, Array(5,15,19), Array(3,2,1))
      println(LocalVector.innerProdOfVectors(v1, v2))
      println(LocalVector.add(v1, v2))
  }

}
