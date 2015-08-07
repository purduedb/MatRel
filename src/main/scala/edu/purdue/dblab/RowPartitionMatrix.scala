package edu.purdue.dblab

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yongyangyu on 6/25/15.
 * The trade-off here is that the number of cols < Int.MaxValue;
 * otherwise the matrix needs to be presented in a block partitioned matrix.
 */
/*
 * Entry(row, col, value) encodes an entry of a matrix
 */
case class Entry(row: Long, col: Long, value: Double)

case class Row(ridx: Long, rvec: Vector)

class RowPartitionMatrix (
      val rows: RDD[Row],
      private var _nrows: Long,
      private var _ncols: Int) extends Matrix with Logging{

    override def nRows(): Long = {
        if (_nrows <= 0) {
            _nrows = rows.map(row => row.ridx).max() + 1
        }
        _nrows
    }

    override def nCols(): Long = {
        if (_ncols <= 0) {
            _ncols = rows.map(row => row.rvec).map(vec => vec.size).max() + 1
        }
        _ncols
    }

    override def nnz(): Long = {
        rows.map(row => row.rvec).map(vec => vec.numNonzeros).aggregate(0L)(_ + _, _ + _)
    }

    def display() = {
        rows.foreach { row =>
            println(s"Rid=${row.ridx}, ${row.rvec.toString}")
        }
    }

    def transpose(): ColumnPartitionMatrix = {
        val colRdd = rows.map {row => Column(row.ridx, row.rvec)}
        new ColumnPartitionMatrix(colRdd, nCols().toInt, nRows())
    }

    def t: ColumnPartitionMatrix = transpose()

    def %*%(dvec: DistributedVector, numPart: Int = 8): DistributedVector = {
        multiplyDvec(dvec, numPart)
    }

    def multiplyDvec(dvec: DistributedVector, numPart: Int = 8): DistributedVector = {
        val colSize = nCols()
        val vecSize = dvec.size.toInt
        require(colSize == vecSize, s"column dimension has be equal to the dimension of the vector but found colSize=$colSize, vecSize=$vecSize")
        val rowPair = rows.map(row => (row.ridx, row.rvec))
        val existsPartitioner = (p: Partitioner) => p match {
            case p: MatrixRangePartitioner => true
            case p: MatrixRangeGreedyPartitioner => true
            case _ => false
        }
        var rowRepartition = rowPair
        if (!rowPair.partitioner.exists(existsPartitioner)) {
            rowRepartition = rowPair.partitionBy(new MatrixRangeGreedyPartitioner(numPart, rowPair))
        }
        // val vecRdd = rowRepartition.map(x => dvEntry(x._1, dvec.inner(x._2)))
        // since nested rdd operation is not supported, have to use a nasty way
        /*  Option 1:
         *  rdd1 = rdd[(rid, vector)] -> rdd[(rid, (cid, v1))] -> rdd[(cid, (rid, v1))]
         *  rdd2 = rdd[(id, v2)]
         *  rdd1.join(rdd2) --> rdd[(cid, ((rid, v1), v2))] -> rdd[(rid, v1 * v2)] (reduceByKey)
         *
         *  Option 2:
         *  broadcast dvec as a vector
         */
        require(vecSize <= Int.MaxValue, s"distributed vector size too large for row partitioned matrix, $vecSize")
        // TODO: change map to mapPartitions
        //val vecRdd = rowRepartition.map(x => (x._1, LocalVector.innerProdOfVectors(localVec, x._2)))
        //                .map(x => dvEntry(x._1, x._2))
        // create a vector from the dvec and broadcast it
        val vecEntries = dvec.entries.map(x => (x.idx, x.v)).collect().sorted
        val arr = Array.fill(dvec.size.toInt)(0.0)
        for (i <- 0 until vecEntries.length) {
            if (i == vecEntries(i)._1.toInt) {
                arr(i) = vecEntries(i)._2
            }
        }
        val bcast = dvec.entries.context.broadcast(arr)
        val partitionFunc = (iter: Iterator[(Long, Vector)]) => {
            iter.map(x => dvEntry(x._1, LocalVector.innerProdOfVectors(x._2, Vectors.dense(bcast.value))))
        }
        val vecRdd = rowRepartition.mapPartitions(partitionFunc)
        new DistributedVector(vecRdd, nRows())
    }

    def %*%(vec: Vector): Vector = {
        multiplyVec(vec)
    }

    def multiplyVec(vec: Vector, numPart: Int = 8): Vector = {
        val colSize = nCols().toInt
        val vecSize = vec.size
        require(colSize == vecSize, s"column dimension has be equal to the dimension of the vector but found colSize=$colSize, vecSize=$vecSize")
        val rowPair = rows.map(row => (row.ridx, row.rvec))
        val existsPartitioner = (p: Partitioner) => p match {
            case p: MatrixRangePartitioner => true
            case p: MatrixRangeGreedyPartitioner => true
            case _ => false
        }
        var rowRepartition = rowPair
        if(!rowPair.partitioner.exists(existsPartitioner)) {
            rowRepartition = rowPair.partitionBy(new MatrixRangeGreedyPartitioner(numPart, rowPair))//rowPair.partitionBy(new RowRangePartitioner(numPart, rowPair.count()))

        }
        val bcast = rowRepartition.context.broadcast(vec.toArray)
        // TODO: change map to mapPartitions
        val partitionFunc = (iter: Iterator[(Long, Vector)]) => {
            iter.map(x => (x._1, LocalVector.innerProdOfVectors(x._2, Vectors.dense(bcast.value))))
        }
        val resVec = rowRepartition.mapPartitions(partitionFunc)
        //val resVec = rowRepartition.map { x =>
        //    (x._1, LocalVector.innerProdOfVectors(x._2, Vectors.dense(bcast.value)))
        //}
        val sortVec = resVec.collect().sorted
        var index = new Array[Int](sortVec.length)
        var values = new Array[Double](sortVec.length)
        for (i <- 0 until sortVec.length) {
            index(i) = sortVec(i)._1.toInt
            values(i) = sortVec(i)._2
        }
        Vectors.sparse(nRows().toInt, index, values)
    }

    def normalizedRow(): RowPartitionMatrix = {
        val nrows = nRows()
        val ncols = nCols()
        val normRdd = rows.map(row => (row.ridx, row.rvec)).map{ row =>
            val vec = row._2
            (row._1, LocalVector.multiplyScalar(1.0 / Vectors.norm(vec, 1.0), vec))
        }.map(x => Row(x._1, x._2))
        new RowPartitionMatrix(normRdd, nrows, ncols.toInt)
    }

    def %*%(other: ColumnPartitionMatrix): RowPartitionMatrix = {
        multiply(other)
    }

    def multiply(other: ColumnPartitionMatrix, numPart: Int = 8): RowPartitionMatrix = {
        require(nCols() == other.nRows(), s"imcompatiable matrices cannot multiply together, " +
        s"#cols = ${nCols()}, #rows = ${other.nRows()}")
        require(other.nCols() < Int.MaxValue, s"cannot store result in a row partition matrix, use block partition matirx instead")
        val rowPair = rows.map(row => (row.ridx, row.rvec))
        val existsPartitioner = (p: Partitioner) => p match {
            case p: MatrixRangePartitioner => true
            case p: MatrixRangeGreedyPartitioner => true
            case _ => false
        }
        var rowRepartition = rowPair
        if(!rowPair.partitioner.exists(existsPartitioner)) {
            rowRepartition = rowPair.partitionBy(new MatrixRangeGreedyPartitioner(numPart, rowPair))//rowPair.partitionBy(new RowRangePartitioner(numPart, rowPair.count()))
        }
        val colPair = other.cols.map(col => (col.cid, col.cvec))
        val rowRdd = rowPair.cartesian(colPair).map { x =>
            val (rid, rvec) = (x._1._1, x._1._2)
            val (cid, cvec) = (x._2._1, x._2._2)
            (rid, (cid, LocalVector.innerProdOfVectors(rvec, cvec)))
        }.groupByKey().map { row =>
            val rid = row._1
            val rowArr = row._2.toArray.sorted
            val arr = Array.fill(rowArr.length)(0.0)
            for (i <- 0 until rowArr.length) {
                arr(i) = rowArr(i)._2
            }
            Row(rid, Vectors.dense(arr))
        }
        new RowPartitionMatrix(rowRdd, nRows(), other.nCols().toInt)
    }
}


object RowPartitionMatrix {
    def createFromCoordinateEntries(entries: RDD[Entry]): RowPartitionMatrix = {
        // first check col size of the coordinate entries
        val colSize = entries.map(x => x.col).max() + 1  // size = max index + 1
        require(colSize <= Int.MaxValue, s"column size too large, colSize = $colSize")
        val rowSize = entries.map(x => x.row).max() + 1
        val rows = entries.groupBy(entry => entry.row)
        val rowRdd = rows.map { row =>
            val ridx = row._1
            val colIter = row._2.toIterator
            val index = new ArrayBuffer[Int]()
            val values = new ArrayBuffer[Double]()
            while (colIter.hasNext) {
                val entry = colIter.next()
                index += entry.col.toInt
                values += entry.value
            }
            Row(ridx, Vectors.sparse(colSize.toInt, index.toArray, values.toArray))
        }
        new RowPartitionMatrix(rowRdd, rowSize, colSize.toInt)
    }
    /*
     *  create a PageRank matrix from a (r, c)-represented adjacency matrix
     *  matrix has to be square
     */
    def PageRankMatrixFromCoordinateEntries(entries: RDD[Entry]): RowPartitionMatrix = {
        val weightRdd = entries.map(entry => (entry.row, (entry.col, entry.value)))
        .groupByKey()
        .map{ x =>
            val r = x._1
            val cnt = x._2.size
            (r, 1.0 / cnt)
        }
        val rowDim = entries.map(entry => entry.row).max() + 1
        val colDim = entries.map(entry => entry.col).max() + 1
        val dim = math.max(rowDim, colDim)
        val transposeRdd = entries.map(entry => (entry.row, (entry.col, entry.value)))
        .join(weightRdd)
        .map{ x =>
            val r = x._1
            val c = x._2._1._1
            val v = x._2._1._2
            val w = x._2._2
            (c, (r, v * w))
        }.groupByKey()
        .map { x =>
            val rid = x._1
            val row = x._2.toList.sorted
            val idxes = new Array[Int](row.length)
            val values = new Array[Double](row.length)
            for (i <- 0 until row.length) {
                idxes(i) = row(i)._1.toInt
                values(i) = row(i)._2
            }
            Row(rid, Vectors.sparse(dim.toInt, idxes, values))
        }.cache()
        //println("*********************************")
        //println("|        matrix loaded          |")
        //println("*********************************")
        new RowPartitionMatrix(transposeRdd, dim, dim.toInt)
    }
}

object MatVecTest {
    def main (args: Array[String]) {
        val conf = new SparkConf()
          .setMaster("local[4]")
          .setAppName("Test for mat-vec for pagerank toy example")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.shuffle.consolidateFiles", "true")
          .set("spark.shuffle.compress", "false")
        val sc = new SparkContext(conf)
        val mat = List[(Long, Long)]((0,0), (0,1), (0,2), (0,3), (0, 4), (0, 5), (1, 0), (1, 2),
            (2, 3), (2, 4), (3,1), (3,2), (3, 4), (4, 5), (5, 4))
        //var vec = Vectors.dense(Array[Double](1,1,1,1,1,1))
        val dvRdd = sc.parallelize(List[Long](0,1,2,3,4,5), 3)
        var dvec = DistributedVector.OnesVector(dvRdd)
        dvec = dvec * (1.0 / dvec.norm(1.0))
        //dvec = dvec.multiply(1.0 / dvec.norm(1.0))
        //vec = LocalVector.multiplyScalar(1.0 / Vectors.norm(vec, 1.0), vec)
        //var v = Vectors.dense(Array[Double](1,1,1,1,1,1))
        var dv = DistributedVector.OnesVector(sc.parallelize(List[Long](0,1,2,3,4,5), 3))
        //v = LocalVector.multiplyScalar(1.0 / Vectors.norm(v, 1.0), v)
        dv = dv * (1.0 / dv.norm(1.0))
        //dv = dv.multiply(1.0 / dv.norm(1.0))
        val CooRdd = sc.parallelize(mat, 3).map(x => Entry(x._1, x._2, 1.0))
        val matrix = RowPartitionMatrix.PageRankMatrixFromCoordinateEntries(CooRdd)
        //matrix.display()
        val alpha = 0.85
        for (i <- 0 until 10) {
            //vec = LocalVector.add(LocalVector.multiplyScalar(alpha, matrix.multiplyVec(vec, 2)),
            //    LocalVector.multiplyScalar(1-alpha, v))
            dvec = matrix.%*%(dvec, 2) * alpha + (dv * (1.0 - alpha))
            //dvec = matrix.multiplyDvec(dvec, 2).multiply(alpha).add(dv.multiply(1.0-alpha))
        }
        //println(vec.toString)
        dvec.display()
        sc.stop()
    }
}