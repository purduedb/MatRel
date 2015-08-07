package edu.purdue.dblab

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yongyangyu on 7/25/15.
 */
object PageRankBlockHDFS {
  def main (args: Array[String]) {
    if (args.length < 4) {
      println("Usage: PageRank <master> <graph> <blk_row_size> <blk_col_size> [<iter>]")
      System.exit(1)
    }
    val graphName = "hdfs://hathi-adm.rcac.purdue.edu:8020/user/yu163/" + args(1)
    val blk_row_size = args(2).toInt
    val blk_col_size = args(3).toInt
    var niter = 0
    if (args.length > 4) niter = args(4).toInt else niter = 10
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("PageRank algorithm on block matrices")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.compress", "false")
      .set("spark.cores.max", "32")
      .set("spark.executor.memory", "32g")
    val sc = new SparkContext(conf)
    val coordinateRdd = genCoordinateRdd(sc, graphName)
    val matrix = BlockPartitionMatrix.PageRankMatrixFromCoordinateEntries(coordinateRdd, blk_row_size, blk_col_size)
    val vecRdd = sc.parallelize(BlockPartitionMatrix.onesMatrixList(matrix.nCols(), 1, blk_col_size, blk_col_size), 4)
    var x = new BlockPartitionMatrix(vecRdd, blk_col_size, blk_col_size, matrix.nCols(), 1)
    val v = x
    val alpha = 0.85
    val t1 = System.currentTimeMillis()
    for (i <- 0 until niter) {
      x =  (matrix %*% x) * alpha + (v * (1.0 - alpha), (blk_col_size, blk_col_size))
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
    println(x.topK(5).mkString("\n"))
    val t2 = System.currentTimeMillis()
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
