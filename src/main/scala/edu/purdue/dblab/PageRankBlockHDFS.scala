package edu.purdue.dblab

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yongyangyu on 7/25/15.
 */
object PageRankBlockHDFS {
  def main (args: Array[String]) {
    if (args.length < 1) {
      println("Usage: PageRank <graph> [<iter>]")
      System.exit(1)
    }
    val graphName = "hdfs://hathi-adm.rcac.purdue.edu:8020/user/yu163/" + args(0)
    //val blk_row_size = args(1).toInt
    //val blk_col_size = args(2).toInt
    var niter = 0
    if (args.length > 1) niter = args(1).toInt else niter = 10
    val conf = new SparkConf()
      .setAppName("PageRank algorithm on block matrices")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.compress", "false")
      .set("spark.cores.max", "16")
      .set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    val coordinateRdd = genCoordinateRdd(sc, graphName)
    val blkSize = BlockPartitionMatrix.estimateBlockSize(coordinateRdd)
    val matrix = BlockPartitionMatrix.PageRankMatrixFromCoordinateEntries(coordinateRdd, blkSize, blkSize)
    val vecRdd = sc.parallelize(BlockPartitionMatrix.onesMatrixList(matrix.nCols(), 1, blkSize, blkSize), 4)
    var x = new BlockPartitionMatrix(vecRdd, blkSize, blkSize, matrix.nCols(), 1)
    val v = x
    val alpha = 0.85
    val t1 = System.currentTimeMillis()
    for (i <- 0 until niter) {
      x =  alpha *: (matrix %*% x) + ((1.0 - alpha) *: v, (blkSize, blkSize))
      //x = matrix.multiply(x).multiplyScalar(alpha).add(v.multiplyScalar(1-alpha), (blk_col_size, blk_col_size))
    }
    //x.blocks.count()

    /*val result = Array.fill(x.nRows().toInt)((0L, 0.0))
    val values = x.toLocalMatrix()
    for (i <- 0 until result.length) {
      result(i) = (i.toLong, values(i, 0))
    }
    scala.util.Sorting.stableSort(result, (e1: Tuple2[Long, Double],
                                           e2: Tuple2[Long, Double]) => e1._2 > e2._2)
    for (i <- 0 until 5) {
      println(result(i))
    }*/
    var t2 = System.currentTimeMillis()
    println("t2 - t1 = " + (t2-t1)/1000.0 + "sec")
    println(x.topK(5).mkString("\n"))
    t2 = System.currentTimeMillis()
    println("t2 - t1 = " + (t2-t1)/1000.0 + "sec")
    sc.stop()
  }

  def genCoordinateRdd(sc: SparkContext, graphName: String): RDD[Entry] = {
    val lines = sc.textFile(graphName, 4)
    lines.map { s =>
      val line = s.split("\\s+")
      if (line(0).charAt(0) == '#') {
        Entry(-1, -1, 0.0)
      }
      else {
        Entry(line(0).toLong, line(1).toLong, 1.0)
      }
    }.filter(x => x.row >= 0)
  }
}
