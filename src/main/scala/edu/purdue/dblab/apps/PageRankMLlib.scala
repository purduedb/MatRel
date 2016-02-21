package edu.purdue.dblab.apps

import edu.purdue.dblab.matrix.{Entry, BlockPartitionMatrix, LocalMatrix}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{DenseMatrix => SparkDense, SparseMatrix => SparkSparse}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yongyangyu on 10/27/15.
 */
object PageRankMLlib {
    def main (args: Array[String]){
      if (args.length < 1) {
        println("Usage: PageRank <graph> [<iter>]")
        System.exit(1)
      }
      val hdfs = "hdfs://10.100.121.126:8022/"
      val graphName = hdfs + args(0)//"hdfs://hathi-adm.rcac.purdue.edu:8020/user/yu163/" + args(0)
      var niter = 0
      if (args.length > 1) niter = args(1).toInt else niter = 10
      val conf = new SparkConf()
        .setAppName("PageRank algorithm on block matrices")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.consolidateFiles", "true")
        .set("spark.shuffle.compress", "false")
        .set("spark.cores.max", "80")
        .set("spark.executor.memory", "48g")
        .set("spark.default.parallelism", "200")
        .set("spark.akka.frameSize", "1600")
      conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
      val sc = new SparkContext(conf)
      val coordinatedRDD = genCoordinateRdd(sc, graphName)
      val dim = math.max(coordinatedRDD.map(x => x.i).max, coordinatedRDD.map(x => x.j).max) + 1
      var blkSize = BlockPartitionMatrix.estimateBlockSize(matrixEntryToEntry(coordinatedRDD))
      if (blkSize > 800000) blkSize = 800000
      val coordinateMatrix = new CoordinateMatrix(coordinatedRDD, dim, dim)
      val matrix = coordinateMatrix.toBlockMatrix(blkSize, blkSize)
      val vecRDD = sc.parallelize(0 until matrix.numCols().toInt).map(x => MatrixEntry(x.toLong, 0, 1.0))
      val v = new CoordinateMatrix(vecRDD, dim, 1L).toBlockMatrix(blkSize, blkSize)
      var x = v
      val alpha = 0.85
      for (i <- 0 until niter) {
          x = scalarMul(matrix, alpha).multiply(x).add(scalarMul(v, 1-alpha))
      }
      x.blocks.saveAsTextFile(hdfs + "tmp_result/pagerank")
      Thread.sleep(10000)
    }

  def genCoordinateRdd(sc: SparkContext, graphName: String): RDD[MatrixEntry] = {
    val lines = sc.textFile(graphName, 8)
    lines.map { s =>
      val line = s.split("\\s+")
      if (line(0).charAt(0) == '#') {
        MatrixEntry(-1, -1, 0.0)
      }
      else {
        MatrixEntry(line(0).toLong, line(1).toLong, 1.0)
      }
    }.filter(x => x.i >= 0)
  }

  def matrixEntryToEntry(rdd1: RDD[MatrixEntry]): RDD[Entry] = {
      rdd1.map(x => Entry(x.i, x.j, x.value))
  }

  def scalarMul(mat: BlockMatrix, c: Double): BlockMatrix = {
      val rdd = mat.blocks.map{ case ((i,j), mat) =>
          val resMat = mat match {
            case den: SparkDense => LocalMatrix.SparkMatrixMultScalar(den, c)
            case sp: SparkSparse => LocalMatrix.SparkMatrixMultScalar(sp, c)
          }
        ((i, j), resMat)
      }
      new BlockMatrix(rdd, mat.rowsPerBlock, mat.colsPerBlock, mat.numRows(), mat.numCols())
  }
}
