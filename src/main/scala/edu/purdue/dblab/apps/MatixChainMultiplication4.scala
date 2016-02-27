package edu.purdue.dblab.apps

import edu.purdue.dblab.matrix.{BlockPartitionMatrix, Entry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkException, SparkContext, SparkConf}

/**
  * Created by yongyangyu on 2/26/16.
  */
object MatixChainMultiplication4 {
    def main(args: Array[String]) {
        if (args.length < 13) {
            println("Usage: matrix1 row1 col1 matrix2 row2 col2 matrix3 row3 col3 matrix4 row4 col4 planNo.")
            System.exit(1)
        }
        val hdfs = "hdfs://10.100.121.126:8022/"
        val matrixName1 = hdfs + args(0)
        val (m1, n1) = (args(1).toLong, args(2).toLong)
        val matrixName2 = hdfs + args(3)
        val (m2, n2) = (args(4).toLong, args(5).toLong)
        val matrixName3 = hdfs + args(6)
        val (m3, n3) = (args(7).toLong, args(8).toLong)
        val matrixName4 = hdfs + args(9)
        val (m4, n4) = (args(10).toLong, args(11).toLong)
        val plan = args(12).toInt
        val conf = new SparkConf()
                  .setAppName("Matrix multiplication chain of size 4")
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
        val blkSize = BlockPartitionMatrix.estimateBlockSizeWithDim(m1, n1)
        println(s"blkSize = $blkSize")
        val matrix1 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD1, blkSize, blkSize, m1, n1)
        val matrix2 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD2, blkSize, blkSize, m2, n2)
        val matrix3 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD3, blkSize, blkSize, m3, n3)
        val matrix4 = BlockPartitionMatrix.createFromCoordinateEntries(coordRDD4, blkSize, blkSize, m4, n4)
        plan match {
            case 1 =>
                val t1 = System.currentTimeMillis()
                val res1 = matrix1 %*% matrix2 %*% matrix3 %*% matrix4
                res1.saveAsTextFile(hdfs + "tmp_result/mult/res1")
                val t2 = System.currentTimeMillis()
                println("t2 - t1 = " + (t2 - t1)/1000.0 + " sec for ((A1*A2)*A3)*A4")
            case 2 =>
                val t1 = System.currentTimeMillis()
                val res2 = (matrix1 %*% (matrix2 %*% matrix3)) %*% matrix4
                res2.saveAsTextFile(hdfs + "tmp_result/mult/res2")
                val t2 = System.currentTimeMillis()
                println("t2 - t1 = " + (t2-t1)/1000.0 + " sec for (A1*(A2*A3))*A4")
            case 3 =>
                val t1 = System.currentTimeMillis()
                val res3 = matrix1 %*% (matrix2 %*% matrix3 %*% matrix4)
                res3.saveAsTextFile(hdfs + "tmp_result/mult/res3")
                val t2 = System.currentTimeMillis()
                println("t2 - t1 = " + (t2-t1)/1000.0 + " sec for A1*(A2*A3*A4)")
            case 4 =>
                val t1 = System.currentTimeMillis()
                val res4 = matrix1 %*% (matrix2 %*% (matrix3 %*% matrix4))
                res4.saveAsTextFile(hdfs + "tmp_result/mult/res4")
                val t2 = System.currentTimeMillis()
                println("t2 - t1 = " + (t2-t1)/1000.0 + " sec for A1*(A2*(A3*A4))")
            case 5 =>
                val t1 = System.currentTimeMillis()
                val res5 = (matrix1 %*% matrix2) %*% (matrix3 %*% matrix4)
                res5.saveAsTextFile(hdfs + "tmp_result/mult/res5")
                val t2 = System.currentTimeMillis()
                println("t2 - t1 = " + (t2-t1)/1000.0 + " sec for (A1*A2)*(A3*A4)")
            case _ =>
                throw new SparkException("planNo. out of range!")
        }
    }

    def genCoordinateRdd(sc: SparkContext, matrixName: String): RDD[Entry] = {
        val lines = sc.textFile(matrixName, 8)
        lines.map { s=>
            val line = s.split("\\s+")
            if (line(0).charAt(0) == '#')
                Entry(-1, -1, 0.0)
            else
                Entry(line(0).toLong, line(1).toLong, 1.0)
        }.filter(x => x.row >= 0)
    }
}
