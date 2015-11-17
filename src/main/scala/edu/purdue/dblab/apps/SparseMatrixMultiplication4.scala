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
    val hdfs = "hdfs://hathi-adm.rcac.purdue.edu:8020/user/yu163/";
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
      .set("spark.cores.max", "64")
      .set("spark.executor.memory", "6g")
      .set("spark.default.parallelism", "256")
      .set("spark.akka.frameSize", "64")
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
    val t1 = System.currentTimeMillis()
    var res: BlockPartitionMatrix = null
    val res23 = matrix2 %*% matrix3
    matrix1.sample(0.1)
    res23.sample(0.1)
    matrix4.sample(0.1)
    var cost1 = 0L
    var cost2 = 0L
    for ((k, v) <- matrix1.colBlkMap) {
        if (res23.rowBlkMap.contains(k)) {
            println(s"A.colMap($k)->$v")
            println(s"res23.rowMap($k)->${res23.rowBlkMap(k)}")
            cost1 += v.toLong * res23.rowBlkMap(k).toLong
        }
    }
    for ((k, v) <- res23.colBlkMap) {
        if (matrix4.rowBlkMap.contains(k)) {
            println(s"res23.colMap($k)->$v")
            println(s"matrix4.rowMap($k)->${matrix4.rowBlkMap(k)}")
            cost2 += v.toLong * matrix4.rowBlkMap(k).toLong
        }
    }

    if (cost1 <= cost2) {
       res = matrix1 %*% res23 %*% matrix4
       println("A*(B*C)*D")
    }
    else {
       res = matrix1 %*% (res23 %*% matrix4)
       println("A*((B*C)*D)")
    }
    println("first block size = " + res.blocks.first().toString().length)
    val t2 = System.currentTimeMillis()
    println("t2 - t1 = " + (t2-t1)/1000.0 + " sec" + " for A*B*C*D")

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
