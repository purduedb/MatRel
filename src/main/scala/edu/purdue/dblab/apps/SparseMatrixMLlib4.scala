package edu.purdue.dblab.apps

/**
  * Created by yongyangyu on 11/3/15.
  */
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object SparseMatrixMLlib4 {
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
    val matrixName4 = hdfs +args(9)
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
    val coordRDD1 = genCoordinateRdd(sc, matrixName1).repartition(20)
    val coordRDD2 = genCoordinateRdd(sc, matrixName2).repartition(20)
    val coordRDD3 = genCoordinateRdd(sc, matrixName3).repartition(20)
    val coordRDD4 = genCoordinateRdd(sc, matrixName4).repartition(20)
    val size = 2000
    val mat1 = new CoordinateMatrix(coordRDD1, m1, n1).toBlockMatrix(size, size)
    val mat2 = new CoordinateMatrix(coordRDD2, m2, n2).toBlockMatrix(size, size)
    val mat3 = new CoordinateMatrix(coordRDD3, m3, n3).toBlockMatrix(size, size)
    val mat4 = new CoordinateMatrix(coordRDD4, m4, n4).toBlockMatrix(size, size)
    val t1 = System.currentTimeMillis()
    val res = mat1.multiply(mat2).multiply(mat3).multiply(mat4)
    println("first block size = " + res.blocks.first().toString().length)
    val t2 = System.currentTimeMillis()
    println("t2 - t1 = " + (t2-t1)/1000.0 + "sec" + " for A*B*C*D")
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
}
