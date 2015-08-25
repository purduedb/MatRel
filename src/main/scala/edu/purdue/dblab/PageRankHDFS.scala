package edu.purdue.dblab

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg._

/**
 * Created by yongyangyu on 7/25/15.
 */
object PageRankHDFS {
  def main (args: Array[String]) {
    if (args.length < 1) {
      println("Usage: PageRank <graph> [<iter>]")
      System.exit(1)
    }
    val graphName = "hdfs://hathi-adm.rcac.purdue.edu:8020/user/yu163/" + args(0)
    var niter = 0
    if (args.length > 1) niter = args(1).toInt else niter = 10
    val conf = new SparkConf()
      .setAppName("PageRank algorithm")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.compress", "false")
      .set("spark.cores.max", "16")
      .set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    val coordinateRdd = genCoordinateRdd(sc, graphName)
    val matrix = RowPartitionMatrix.PageRankMatrixFromCoordinateEntries(coordinateRdd)
    //val matrix = ColumnPartitionMatrix.PageRankMatrixFromCoordinateEntries(coordinateRdd)
    var x = Vectors.dense(Array.fill[Double](matrix.nCols().toInt)(1.0))
    //x = LocalVector.multiplyScalar(1.0 / Vectors.norm(x, 1.0), x)
    //var y = DistributedVector.OnesVector(sc.parallelize((0L until matrix.nRows()).toList))
    val v = x
    //y = y.multiply(1.0 / y.norm(1.0))
    val alpha = 0.85
    val t1 = System.currentTimeMillis()
    for (i <- 0 until niter) {
      x = LocalVector.add(LocalVector.multiplyScalar(alpha, matrix.multiplyVec(x, 4)),
        LocalVector.multiplyScalar(1-alpha, v))
      //y = matrix.multiplyDvec(y, 8).multiply(alpha).add(v.multiply(1.0-alpha))
    }
    val t2 = System.currentTimeMillis()
    println("t2 - t1 = " + (t2-t1)/1000.0 + "sec")
    //val result = y.entries.collect()
    val result = Array.fill(x.size)((0L, 0.0))
    val values = x.toArray
    for (i <- 0 until result.length) {
      result(i) = (i.toLong, values(i))
    }

    scala.util.Sorting.stableSort(result, (e1: Tuple2[Long, Double],
                                           e2: Tuple2[Long, Double]) => e1._2 > e2._2)
    //scala.util.Sorting.stableSort(result, (e1: dvEntry, e2: dvEntry) => e1.v > e2.v)
    //val t2 = System.currentTimeMillis()
    //println((t2-t1)/1000.0 + "sec")
    for (i <- 0 until 5) {
      //println(result(i).idx + ", " + result(i).v)
      println(result(i))
    }
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
