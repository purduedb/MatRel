package edu.purdue.dblab

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yongyangyu on 10/13/15.
 */
object SparseMatrixMultiplication {
     def main (args: Array[String]){
       /*val conf = new SparkConf()
         .setMaster("local[4]")
         .setAppName("Test for block partition matrices")
         .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .set("spark.shuffle.consolidateFiles", "true")
         .set("spark.shuffle.compress", "false")
         .set("spark.executor.memory", "2g")*/
       if (args.length < 9) {
          println("Usage: matrix1 row1 col1 matrix2 row2 col2 matrix3 row3 col3")
          System.exit(1)
       }
       val matrixName1 = args(0)
       val (m1, n1) = (args(1).toLong, args(2).toLong)
       val matrixName2 = args(3)
       val (m2, n2) = (args(4).toLong, args(5).toLong)
       val matrixName3 = args(6)
       val (m3, n3) = (args(7).toLong, args(8).toLong)
       val conf = new SparkConf()
         .setAppName("Three matrices multiplication chain")
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
       val blkSize = BlockPartitionMatrix.estimateBlockSize(coordRDD1)
       val matrix1 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD1, blkSize, blkSize, m1, n1)
       val matrix2 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD2, blkSize, blkSize, m2, n2)
       val matrix3 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD3, blkSize, blkSize, m3, n3)

       var t1 = System.currentTimeMillis()
       val res1 = matrix1 %*% matrix2 %*% matrix3
       var t2 = System.currentTimeMillis()
       println("t2 - t1 = " + (t2-t1)/1000.0 + "sec" + " for A*B*C")
       res1.nCols()
       t1 = System.currentTimeMillis()
       val res2 = matrix1 %*% (matrix2 %*% matrix3)
       t2 = System.currentTimeMillis()
       println("t2 - t1 = " + (t2-t1)/1000.0 + "sec" + " for A*(B*C)")
       res2.nCols()
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
