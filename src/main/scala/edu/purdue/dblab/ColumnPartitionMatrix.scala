package edu.purdue.dblab

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.{Matrix => MLMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.Partitioner

/**
 * Created by yongyangyu on 7/6/15.
 * Each column is a represented by a vector.
 * This is a natural representation than row partition matrix.
 */

case class Column(cid: Long, cvec: Vector)

class ColumnPartitionMatrix (
      val cols: RDD[Column],
      private var _nrows: Int,
      private var _ncols: Long) extends Matrix with Logging {

    override def nRows(): Long = {
        if (_nrows <= 0) {
            _nrows = cols.map(col => col.cvec).map(vec => vec.size).max() + 1
        }
        _nrows
    }

    override def nCols(): Long = {
        if (_ncols <= 0) {
            _ncols = cols.map(col => col.cid).max() + 1
        }
        _ncols
    }

    override def nnz(): Long = {
        cols.map{col => col.cvec}.map(vec => vec.numNonzeros).aggregate(0L)(_ + _, _ + _)
    }

    def display() = {
        cols.foreach{ col =>
            println(s"Cid=${col.cid}, ${col.cvec}")
        }
    }

    def transpose(): RowPartitionMatrix = {
        val rowRdd = cols.map (col => Row(col.cid, col.cvec))
        new RowPartitionMatrix(rowRdd, nCols(), nRows().toInt)
    }

    def t: RowPartitionMatrix = transpose()

    def %*%(dvec: DistributedVector, numPart: Int = 8): DistributedVector = {
        multiplyDvec(dvec, numPart)
    }

    def multiplyDvec(dvec: DistributedVector, numPart: Int = 8): DistributedVector = {
        val colSize = nCols()
        val vecSize = dvec.size
        require(colSize == vecSize, s"column dimension has be equal to the dimension of the vector but found colSize=$colSize, vecSize=$vecSize")
        val colPair = cols.map(col => (col.cid, col.cvec))
        val existsPartitioner = (p: Partitioner) => p match {
            case p: MatrixRangePartitioner => true
            case p: MatrixRangeGreedyPartitioner => true
            case _ => false
        }
        var colRepartition = colPair
        if (!colPair.partitioner.exists(existsPartitioner)) {
            colRepartition = colPair.partitionBy(new MatrixRangeGreedyPartitioner(numPart, colPair))
        }
        require(vecSize <= Int.MaxValue, s"distributed vector size too large for column partitioned matrix, $vecSize")
        val vecRdd = dvec.entries.map(entry => (entry.idx, entry.v))
        val prodVec = colRepartition.join(vecRdd).map { x =>
          val vi = x._2._2
          val vec = x._2._1
          LocalVector.multiplyScalar(vi, vec)
        }.fold(Vectors.zeros(nRows().toInt))(LocalVector.add(_, _))
        val result = new Array[dvEntry](prodVec.size)
        for (i <- 0 until prodVec.size) {
            result(i) = dvEntry(i, prodVec(i))
        }
        new DistributedVector(cols.context.parallelize(result), nRows())
    }

    def %*%(vec: Vector): Vector = {
        multiplyVec(vec)
    }

    def multiplyVec(vec: Vector, numPart: Int = 8): Vector = {
        val colSize = nCols()
        val vecSize = vec.size
        require(colSize == vecSize, s"column dimension has be equal to the dimension of the vector but found colSize=$colSize, vecSize=$vecSize")
        val colPair = cols.map(col => (col.cid, col.cvec))
        val existsPartitioner = (p: Partitioner) => p match {
          case p: MatrixRangePartitioner => true
          case p: MatrixRangeGreedyPartitioner => true
          case _ => false
        }
        var colRepartition = colPair
        if (!colRepartition.partitioner.exists(existsPartitioner)) {
            colRepartition = colPair.partitionBy(new MatrixRangeGreedyPartitioner(numPart, colPair))
        }
        val bcast = cols.context.broadcast(vec.toArray)
        cols.map { x =>
            val cid = x.cid
            val arr = bcast.value
            LocalVector.multiplyScalar(arr(cid.toInt), x.cvec)
        }.fold(Vectors.zeros(nRows().toInt))(LocalVector.add(_, _))

        /*val idx = vec.toSparse.indices
        val vs = vec.toSparse.values
        val dv = Array.fill(idx.length)((0L, 0.0))
        for (i <- 0 until idx.length) {
            dv(i) = (idx(i), vs(i))
        }
        val dvRdd = cols.context.parallelize(dv)
        colRepartition.join(dvRdd).map { x =>
            val vi = x._2._2
            val vec = x._2._1
            println("cvec size = " + vec.size)
            LocalVector.multiplyScalar(vi, vec)
        }.fold(Vectors.dense(Array.fill(nRows().toInt)(0.0)))(LocalVector.add(_, _)) */
    }

    def %*%(other: RowPartitionMatrix): MLMatrix = {
        multiply(other)
    }

    def multiply(other: RowPartitionMatrix): MLMatrix = {
        require(nCols() == other.nRows(), s"Dimension must match for matrix multiplication, but found " +
        s"col size = ${nCols()}, row size = ${other.nRows()}")
        val colRdd = cols.map(col => (col.cid, col.cvec))
        val rowRdd = other.rows.map(row => (row.ridx, row.rvec))
        val fadd = (m1: MLMatrix, m2: MLMatrix) => {
            require(m1.numRows == m2.numRows, s"row dimension must be the same, but found m1.numRows = ${m1.numRows}, m2.numRows = ${m2.numRows}")
            require(m1.numCols == m2.numCols, s"col dimension must be the same, but found m1.numCols - ${m1.numCols}, m2.numCols = ${m2.numCols}")
            val arr1 = m1.toArray
            val arr2 = m2.toArray
            val arr = Array.fill(arr1.length)(0.0)
            for (i <- 0 until arr.length) {
                arr(i) = arr1(i) + arr2(i)
            }
            new DenseMatrix(m1.numRows, m1.numCols, arr)
        }
        colRdd.join(rowRdd).map( x =>
            LocalVector.outerProduct(x._2._1, x._2._2)
        ).fold(DenseMatrix.zeros(nRows().toInt, other.nCols().toInt))(fadd(_, _))
    }
}

object ColumnPartitionMatrix {
    /*
     *  create a PageRank matrix (column stochastic matrix) from a (r, c)-represented
     *  adjacency matrix
     *  matrix has to be squre
     */
    def PageRankMatrixFromCoordinateEntries(entries: RDD[Entry]): ColumnPartitionMatrix = {
        val entryRdd = entries.map(entry => (entry.row, (entry.col, entry.value))).cache()
        val weightRdd = entryRdd
                        .groupByKey()
                        .map { x =>
                            val r = x._1
                            val cnt = x._2.size
                            (r, 1.0 / cnt)
                        }
        val rowDim = entries.map(entry => entry.row).max() + 1
        val colDim = entries.map(entry => entry.col).max() + 1
        val dim = math.max(rowDim, colDim)
        val colRdd = entryRdd
                     .join(weightRdd)
                     .map { x =>
                        val r = x._1
                        val c = x._2._1._1
                        val v = x._2._1._2
                        val w = x._2._2
                        (r, (c, v * w))
                      }.groupByKey()
                      .map { x =>
                        val cid = x._1
                        val col = x._2.toList.sorted
                        val idxes = new Array[Int](col.length)
                        val values = new Array[Double](col.length)
                        for (j <- 0 until col.length) {
                            idxes(j) = col(j)._1.toInt
                            values(j) = col(j)._2
                        }
                        Column(cid, Vectors.sparse(dim.toInt, idxes, values))
                      }.cache()
      //println("*********************************")
      //println("|        matrix loaded          |")
      //println("*********************************")
      new ColumnPartitionMatrix(colRdd, dim.toInt, dim)
    }
}

object Test {
    def main (args: Array[String]) {
      val conf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("Test for mat-vec for pagerank toy example")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.consolidateFiles", "true")
        .set("spark.shuffle.compress", "false")
      val sc = new SparkContext(conf)
      val mat = List[(Long, Long)]((0,1), (0,2), (0,3), (0, 4), (0, 5), (1, 0), (1, 2),
        (2, 3), (2, 4), (3,1), (3,2), (3, 4), (4, 5), (5, 4))
      val dvRdd = sc.parallelize(List[Long](0,1,2,3,4,5), 3)
      var dvec = DistributedVector.OnesVector(dvRdd)
      dvec = dvec * (1.0 / dvec.norm(1.0))
      //dvec = dvec.multiply(1.0 / dvec.norm(1.0))
      var dv = DistributedVector.OnesVector(sc.parallelize(List[Long](0,1,2,3,4,5), 3))
      dv = dv * (1.0 / dv.norm(1.0))
      //dv = dv.multiply(1.0 / dv.norm(1.0))
      val CooRdd = sc.parallelize(mat, 3).map(x => Entry(x._1, x._2, 1.0))
      val matrix = ColumnPartitionMatrix.PageRankMatrixFromCoordinateEntries(CooRdd)
      //matrix.cols.foreach(x => println(x.toString))
      val alpha = 0.85
      for (i <- 0 until 10) {
          dvec = matrix.%*%(dvec, 2) * alpha + (dv * (1.0 - alpha))
          //dvec = matrix.multiplyDvec(dvec, 2).multiply(alpha).add(dv.multiply(1.0-alpha))
      }
      dvec.display()
      sc.stop()
  }
}