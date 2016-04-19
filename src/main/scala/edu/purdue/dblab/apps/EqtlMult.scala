package edu.purdue.dblab.apps

import edu.purdue.dblab.matrix.{BlockPartitionMatrix, DenseMatrix, MLMatrix, SparseMatrix}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by yongyangyu on 4/18/16.
  * add this experiment for comparing with ScaLAPACK and SciDB for multiplying two matrix partitions
  * from the eQTL genome dataset.
  */
object EqtlMult {
    def main(args: Array[String]) {
        if (args.length < 7) {
            println("Usage: geno_matrix m1 n1 mrna_matrix m2 n2 isSparse")
            System.exit(1)
        }
        val hdfs = "hdfs://10.100.121.126:8022/"
        val matrixG = hdfs + args(0)
        val (m1, n1) = (args(1).toLong, args(2).toLong)
        val matrixM = hdfs + args(3)
        val (m2, n2) = (args(4).toLong, args(5).toLong)
        var isSparse = true
        if (args(6).toInt == 0) isSparse = false
        val conf = new SparkConf()
                          .setAppName("eQTL Multiplication test")
                          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                          .set("spark.shuffle.consolidateFiles", "true")
                          .set("spark.shuffle.compress", "false")
                          .set("spark.cores.max", "64")
                          .set("spark.executor.memory", "58g")
                          .set("spark.akka.frameSize", "64")
        conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
        val sc = new SparkContext(conf)
        val mrnaSize = BlockPartitionMatrix.estimateBlockSizeWithDim(m2, n2)
        val mrna = BlockPartitionMatrix.createDenseBlockMatrix(sc, matrixM, mrnaSize, mrnaSize,
            m2, n2, 64)
        mrna.cache()
        val genoSize = BlockPartitionMatrix.estimateBlockSizeWithDim(m1, n1)
        var geno = BlockPartitionMatrix.createDenseBlockMatrix(sc, matrixG, genoSize, genoSize,
            m1, n1, 64)
        if (isSparse) {
            val rdd = geno.blocks.map{ case ((i, j), mat) =>
            val v = mat match {
                    case m: DenseMatrix => m.toSparse
                    case msp: SparseMatrix => msp
                }
                ((i, j), v.asInstanceOf[MLMatrix])
            }
            geno = new BlockPartitionMatrix(rdd, genoSize, genoSize, m1, n1)
        }
        val S = mrna %*% geno.t
        S.saveAsTextFile(hdfs + "tmp_result/eqtl")
        Thread.sleep(10000)
    }
}
