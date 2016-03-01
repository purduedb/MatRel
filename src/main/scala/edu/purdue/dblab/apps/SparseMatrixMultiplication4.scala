package edu.purdue.dblab.apps

/**
  * Created by yongyangyu on 11/3/15.
  */

import edu.purdue.dblab.matrix.{BlockPartitionMatrix, Entry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparseMatrixMultiplication4 {
  def main (args: Array[String]){
    if (args.length < 12) {
      println("Usage: matrix1 row1 col1 matrix2 row2 col2 matrix3 row3 col3 matrix4 row4 col4")
      System.exit(1)
    }
    val hdfs = "hdfs://10.100.121.126:8022/"//"hdfs://hathi-adm.rcac.purdue.edu:8020/user/yu163/"
    val matrixName1 = hdfs + args(0)
    val (m1, n1) = (args(1).toLong, args(2).toLong)
    val matrixName2 = hdfs + args(3)
    val (m2, n2) = (args(4).toLong, args(5).toLong)
    val matrixName3 = hdfs + args(6)
    val (m3, n3) = (args(7).toLong, args(8).toLong)
    val matrixName4 = hdfs + args(9)
    val (m4, n4) = (args(10).toLong, args(11).toLong)
    val conf = new SparkConf()
      .setAppName("Four matrices multiplication chain")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.compress", "false")
      .set("spark.cores.max", "80")
      .set("spark.executor.memory", "20g")
      .set("spark.default.parallelism", "200")
      .set("spark.akka.frameSize", "512")
    conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
    val sc = new SparkContext(conf)
    val coordRDD1 = genCoordinateRdd(sc, matrixName1)
    val coordRDD2 = genCoordinateRdd(sc, matrixName2)
    val coordRDD3 = genCoordinateRdd(sc, matrixName3)
    val coordRDD4 = genCoordinateRdd(sc, matrixName4)
    val size = BlockPartitionMatrix.estimateBlockSizeWithDim(m1, n1)
    println("blkSize = " + size)
    val matrix1 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD1, size, size, m1, n1)
    val matrix2 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD2, size, size, m2, n2)
    val matrix3 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD3, size, size, m3, n3)
    val matrix4 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD4, size, size, m4, n4)
    // sample among A1, A2, A3, and A4 to decide which multiplication should be done first
    matrix1.sample(0.1)
    matrix2.sample(0.1)
    matrix3.sample(0.1)
    matrix4.sample(0.1)
    var cost12 = 0L
    for ((k, v) <- matrix1.colBlkMap) {
        if (matrix2.rowBlkMap.contains(k)) {
            cost12 += v.toLong * matrix2.rowBlkMap(k).toLong
        }
    }
    var cost23 = 0L
    for ((k, v) <- matrix2.colBlkMap) {
        if (matrix3.rowBlkMap.contains(k)) {
            cost23 += v.toLong * matrix3.rowBlkMap(k).toLong
        }
    }
    var cost34 = 0L
    for ((k, v) <- matrix3.colBlkMap) {
       if (matrix4.rowBlkMap.contains(k)) {
           cost34 += v.toLong * matrix4.rowBlkMap(k).toLong
       }
    }
    if (cost12 == math.min(math.min(cost12, cost23), cost34)) {
        println("Performing multiplication A1*A2")
        val mat12 = matrix1 %*% matrix2
        mat12.sample(0.1)
        var cost123 = 0L
        for ((k, v) <- mat12.colBlkMap) {
            if (matrix3.rowBlkMap.contains(k)) {
                cost123 += v.toLong * matrix3.rowBlkMap(k).toLong
            }
        }
        if (cost123 <= cost34) {
            println("Performing multiplication (A1*A2)*A3")
            val res = mat12 %*% matrix3 %*% matrix4
            res.saveAsTextFile(hdfs + "tmp_result/mult/res")
        }
        else {
            println("Performing multiplication A3*A4")
            val res = mat12 %*% (matrix3 * matrix4)
            res.saveAsTextFile(hdfs + "tmp_result/mult/res")
        }
    }
    else if (cost23 == math.min(math.min(cost12, cost23), cost34)) {
        println("Performing multiplication A2*A3")
        val mat23 = matrix2 %*% matrix3
        mat23.sample(0.1)
        var cost123 = 0L
        for ((k, v) <- matrix1.colBlkMap) {
            if (mat23.rowBlkMap.contains(k)) {
                cost123 += v.toLong * mat23.rowBlkMap(k).toLong
            }
        }
        var cost234 = 0L
        for ((k, v) <- mat23.colBlkMap) {
            if (matrix4.rowBlkMap.contains(k)) {
                cost234 += v.toLong * matrix4.rowBlkMap(k).toLong
            }
        }
        if (cost123 <= cost234) {
            println("Performing multiplication A1*(A2*A3)")
            val res = matrix1 %*% mat23 %*% matrix4
            res.saveAsTextFile(hdfs + "tmp_result/mult/res")
        }
        else {
            println("Performing multiplication (A2*A3)*A4")
            val res = matrix1 %*% (mat23 %*% matrix4)
            res.saveAsTextFile(hdfs + "tmp_result/mult/res")
        }
    }
    else if (cost34 == math.min(math.min(cost12, cost23), cost34)) {
        println("Performing multiplication A3*A4")
        val mat34 = matrix3 %*% matrix4
        mat34.sample(0.1)
        var cost234 = 0L
        for ((k, v) <- matrix2.colBlkMap) {
            if (mat34.rowBlkMap.contains(k)) {
                cost234 += v.toLong * mat34.rowBlkMap(k).toLong
            }
        }
        if (cost12 <= cost234) {
            println("Performing multiplication (A1*A2)*(A3*A4)")
            val res = matrix1 %*% matrix2 %*% mat34
            res.saveAsTextFile(hdfs + "tmp_result/mult/res")
        }
        else {
            println("Performing mutltiplication A2*(A3*A4)")
            val res = matrix1 %*% (matrix2 %*% mat34)
            res.saveAsTextFile(hdfs + "tmp_result/mutl/res")
        }
    }
    Thread.sleep(10000)
  }

  def genCoordinateRdd(sc: SparkContext, graphName: String): RDD[Entry] = {
    val lines = sc.textFile(graphName, 8)
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
