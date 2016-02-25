package edu.purdue.dblab.apps

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, BlockMatrix}
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkException, SparkContext, SparkConf}

/**
  * Created by yongyangyu on 2/18/16.
  * GNMF implementation for MLlib.
  */
object GNMFMLlib {
    def main(args: Array[String]) {
        if (args.length < 4) {
            println("Usage: GNMF <matrix> <nrows> <ncols> <nfeatures> [<niter>]")
            System.exit(1)
        }
        val hdfs = "hdfs://10.100.121.126:8022/"
        val matrixName = hdfs + args(0)
        val (nrows, ncols) = (args(1).toLong, args(2).toLong)
        val nfeature = args(3).toInt
        var niter = 0
        if (args.length > 4) niter = args(4).toInt else niter = 10
        val conf = new SparkConf()
                      .setAppName("GNMF with MLlib")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .set("spark.shuffle.consolidateFile", "true")
                      .set("spark.shuffle.compress", "false")
                      .set("spark.cores.max", "80")
                      .set("spark.executor.memory", "48g")
                      .set("spark.default.parallelism", "100")
                      .set("spark.akka.frameSize", "1024")
        conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
        val sc = new SparkContext(conf)
        val V = new CoordinateMatrix(getCOORdd(sc, matrixName), nrows, ncols).toBlockMatrix()
        V.blocks.cache() // cache matrix V since it is used multiple times
        val blkSize = (V.rowsPerBlock, V.colsPerBlock)
        if (blkSize._1 != blkSize._2) {
            println(s"non-square blocks, ${blkSize}")
            System.exit(1)
        }
        var W = randomBlockMatrix(sc, nrows, nfeature, blkSize._1)
        var H = randomBlockMatrix(sc, nfeature, ncols, blkSize._1)
        val eps = 1e-8
        // both W and H are small matrices, no big overheads for caching them
        for (i <- 0 until niter) {
            H = elemDivide(elemMultiply(H, W.transpose.multiply(V)), addScala(W.transpose.multiply(W).multiply(H), eps))
            H.blocks.cache()
            W = elemDivide(elemMultiply(W, V.multiply(H.transpose)), addScala(W.multiply(H).multiply(H.transpose), eps))
            W.blocks.cache()
        }
        W.blocks.saveAsTextFile(hdfs + "tmp_result/gnmf")
        H.blocks.saveAsTextFile(hdfs + "tmp_result/gnmf")
        Thread.sleep(10000)
    }

    def getCOORdd(sc: SparkContext, matrixName: String): RDD[MatrixEntry] = {
        val lines = sc.textFile(matrixName, 8)
        lines.map { s =>
            val line = s.split("\\s+")
            MatrixEntry(line(0).toLong - 1, line(1).toLong - 1, line(2).toDouble)
        }
    }

    def rand(nrows: Int, ncols: Int, min: Double, max: Double): Matrix = {
        val arr = new Array[Double](nrows * ncols)
        val random = scala.util.Random
        for (i <-  0 until arr.length) {
            arr(i) = min + (max - min) * random.nextDouble()
        }
        new DenseMatrix(nrows, ncols, arr)
    }

    def randomBlockMatrix(sc: SparkContext,
                          nrows: Long,
                          ncols: Long,
                          blkSize: Int,
                          min: Double = 0.0,
                          max: Double = 1.0): BlockMatrix = {
        val rowBlkCnt = math.ceil(nrows * 1.0 / blkSize).toInt
        val colBlkCnt = math.ceil(ncols * 1.0 / blkSize).toInt
        val idx = for(i <- 0 until rowBlkCnt; j <- 0 until colBlkCnt) yield (i, j)
        val blkId = sc.parallelize(idx)
        val rdd = blkId.map { case(i, j) =>
            val curRow = math.min(blkSize, nrows - i * blkSize).toInt
            val curCol = math.min(blkSize, ncols - j * blkSize).toInt
            ((i, j), rand(curRow, curCol, min, max))
        }
        new BlockMatrix(rdd, blkSize, blkSize, nrows, ncols)
    }

    // we need element-wise multiplication, division, and addition here
    // just provide the straight-forward implementation
    def elemMultiply(left: BlockMatrix, right: BlockMatrix): BlockMatrix = {
        require(left.numRows() == right.numRows(), s"Dimension incompatible A.rows = ${left.numRows()}, " +
          s"B.rows = ${right.numRows()}")
        require(left.numCols() == right.numCols(), s"Dimension incompatible B.cols = ${left.numCols()}, " +
          s"B.cols = ${right.numCols()}")
        require(left.rowsPerBlock == right.rowsPerBlock && left.colsPerBlock == right.colsPerBlock,
          s"Block size not compatible, A.block = (${left.rowsPerBlock}, ${left.colsPerBlock}), " +
          s"B.block = (${right.rowsPerBlock}, ${right.colsPerBlock})")
         val rdd = left.blocks.join(right.blocks).map { case ((i, j), (mat1, mat2)) =>
             ((i, j), localMultiply(mat1, mat2))
         }
        new BlockMatrix(rdd, left.rowsPerBlock, left.colsPerBlock, left.numRows(), left.numCols())
    }

    def localMultiply(mat1: Matrix, mat2: Matrix): Matrix = {
        val arr1 = mat1.toArray
        val arr2 = mat2.toArray
        val arr = new Array[Double](arr1.length)
        for (i <- 0 until arr.length) {
            arr(i) = arr1(1) * arr2(i)
        }
        new DenseMatrix(mat1.numRows, mat1.numCols, arr)
    }

    def elemDivide(left: BlockMatrix, right: BlockMatrix): BlockMatrix = {
        require(left.numRows() == right.numRows(), s"Dimension incompatible A.rows = ${left.numRows()}, " +
          s"B.rows = ${right.numRows()}")
        require(left.numCols() == right.numCols(), s"Dimension incompatible A.cols = ${left.numCols()}, " +
          s"B.cols = ${right.numCols()}")
        require(left.rowsPerBlock == right.rowsPerBlock && left.colsPerBlock == right.colsPerBlock,
            s"Block size not compatible, A.block = (${left.rowsPerBlock}, ${left.colsPerBlock}), " +
            s"B.block = (${right.rowsPerBlock}, ${right.colsPerBlock})")
        val rdd = left.blocks.join(right.blocks).map { case((i, j), (mat1, mat2)) =>
            ((i, j), localDivide(mat1, mat2))
        }
        new BlockMatrix(rdd, left.rowsPerBlock, left.colsPerBlock, left.numRows(), left.numCols())
    }

    def localDivide(mat1: Matrix,  mat2: Matrix): Matrix = {
        val arr1 = mat1.toArray
        val arr2 = mat2.toArray
        val arr = new Array[Double](arr1.length)
        for (i <- 0 until arr.length) {
            if (arr2(i) != 0) {
                arr(i) = arr1(i) / arr2(i)
            }
        }
        new DenseMatrix(mat1.numRows, mat1.numCols, arr)
    }

    def elemAdd(left: BlockMatrix, right: BlockMatrix): BlockMatrix = {
        require(left.numRows() == right.numRows(), s"Dimension incompatible A.rows = ${left.numRows()}, " +
          s"B.rows = ${right.numRows()}")
        require(left.numCols() == right.numCols(), s"Dimension incompatible B.cols = ${left.numCols()}, " +
          s"B.cols = ${right.numCols()}")
        require(left.rowsPerBlock == right.rowsPerBlock && left.colsPerBlock == right.colsPerBlock,
            s"Block size not compatible, A.block = (${left.rowsPerBlock}, ${left.colsPerBlock}), " +
            s"B.block = (${right.rowsPerBlock}, ${right.colsPerBlock})")
        val rdd = left.blocks.fullOuterJoin(right.blocks).map { case (key, (mat1, mat2)) =>
            if (mat1.isDefined && mat2.isDefined) {
                (key, localAdd(mat1.get, mat2.get))
            }
            else if (mat1.isDefined && !mat2.isDefined) {
                (key, mat1.get)
            }
            else if (!mat1.isDefined && mat2.isDefined) {
                (key, mat2.get)
            }
            else {
                throw new SparkException("Both matrices are not defined in local addition")
            }
        }
        new BlockMatrix(rdd, left.rowsPerBlock, left.colsPerBlock, left.numRows(), left.numCols())
    }

    def localAdd(mat1: Matrix, mat2: Matrix): Matrix = {
        val arr1 = mat1.toArray
        val arr2 = mat2.toArray
        val arr = new Array[Double](arr1.length)
        for (i <- 0 until arr.length) {
            arr(i) = arr1(i) + arr2(i)
        }
        new DenseMatrix(mat1.numRows, mat1.numCols, arr)
    }

    def addScala(mat: BlockMatrix, gamma: Double): BlockMatrix = {
        val rdd = mat.blocks.map { case (key, blk) =>
            blk match {
                case dm: DenseMatrix =>
                    val arr = dm.values.clone()
                    for (i <- 0 until arr.length) {
                        arr(i) += gamma
                    }
                    new DenseMatrix(dm.numRows, dm.numCols, arr, dm.isTransposed)
                case sp: SparseMatrix =>
                    val arr = sp.values.clone()
                    for (i <- 0 until arr.length) {
                        arr(i) += gamma
                    }
                    new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, arr, sp.isTransposed)
                case _ => throw new SparkException("matrix type not supported")
            }
            (key, blk)
        }
        new BlockMatrix(rdd, mat.rowsPerBlock, mat.colsPerBlock, mat.numRows(), mat.numCols())
    }
}
