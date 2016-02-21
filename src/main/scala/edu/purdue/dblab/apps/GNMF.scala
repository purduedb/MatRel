package edu.purdue.dblab.apps

import edu.purdue.dblab.matrix.{Entry, BlockPartitionMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yongyangyu on 2/18/16.
  * This app computes Gaussian non-negative matrix factorization.
  * Here, we're testing GNMF for Netflix dataset. The number of hidden features are
  * predefined. This values also affects the performance of the model.
  */
object GNMF {
    def main(args: Array[String]) {
        if (args.length < 4) {
            println("Usage: GNMF <matrix> <nrows> <ncols> <nfeatures> [<niter>]")
            System.exit(1)
        }
        val hdfs  = "hdfs://10.100.121.126:8022/"
        val matrixName = hdfs + args(0)
        val (nrows, ncols) = (args(1).toLong, args(2).toLong)
        val nfeature = args(3).toInt
        var niter = 0
        if (args.length > 4) niter = args(4).toInt else niter = 10
        val conf = new SparkConf()
                      .setAppName("Gaussian non-negative matrix factorization")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerialilzer")
                      .set("spark.shuffle.consolidateFiles", "true")
                      .set("spark.shuffle.compress", "false")
                      .set("spark.cores.max", "80")
                      .set("spark.executor.memory", "48g")
                      .set("spark.default.parallelism", "200")
                      .set("spark.akka.frameSize", "1024")
        conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
        val sc = new SparkContext(conf)
        val blkSize = BlockPartitionMatrix.estimateBlockSizeWithDim(nrows, ncols)
        val V = BlockPartitionMatrix.createFromCoordinateEntries(genCOORdd(sc,
          matrixName), blkSize, blkSize, nrows, ncols)
        var W = BlockPartitionMatrix.randMatrix(sc, nrows, nfeature, blkSize)
        var H = BlockPartitionMatrix.randMatrix(sc, nfeature, ncols, blkSize)
        val eps = 1e-8
        for (i <- 0 until niter) {
            H = H * (W.t %*% V) / (((W.t %*% W) %*% H) + eps)
            W = W * (V %*% H.t) / ((W %*% (H %*% H.t)) + eps)
        }
        W.saveAsTextFile(hdfs + "tmp_result/gnmf")
        H.saveAsTextFile(hdfs + "tmp_result/gnmf")
        Thread.sleep(10000)
    }

    def genCOORdd(sc: SparkContext, matrixName: String): RDD[Entry] = {
        val lines = sc.textFile(matrixName, 8)
        lines.map { s =>
            val line = s.split("\\s+")
            Entry(line(0).toLong - 1, line(1).toLong - 1, line(2).toDouble)
        }
    }
}
