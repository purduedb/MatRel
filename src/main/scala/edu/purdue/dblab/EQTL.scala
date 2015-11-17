package edu.purdue.dblab

import helper.RankData
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import util.Random

/**
  * Created by yongyangyu on 11/16/15.
  */
object EQTL {
  def main(args: Array[String]) {
    if (args.length < 6) {
      println("Usage: geno_matrix m1 n1 mrna_matrix m2 n2")
      System.exit(1)
    }
    val hdfs = "hdfs://hathi-adm.rcac.purdue.edu:8020/user/yu163/"
    val matrixName1 = hdfs + args(0)
    val (m1, n1) = (4, 4)//(args(1).toLong, args(2).toLong)
    val matrixName2 = hdfs + args(3)
    val (m2, n2) = (4, 4)//(args(4).toLong, args(5).toLong)
    val conf = new SparkConf()
      .setAppName("eQTL")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.compress", "false")
      .set("spark.cores.max", "64")
      .set("spark.executor.memory", "6g")
      .set("spark.default.parallelism", "256")
      .set("spark.akka.frameSize", "64")
    conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
    val sc = new SparkContext(conf)
    val MrnaRDD = genMrnaRDD(sc, matrixName2)
    val mrnaSize = BlockPartitionMatrix.estimateBlockSizeWithDim(m2, n2)
    val mrnaRank = BlockPartitionMatrix.createFromCoordinateEntries(MrnaRDD, 2, 2, m2, n2)
    //println("mrnaRank")
    //println(mrnaRank.toLocalMatrix())
    val genoRDD = genGenoRDD(sc, matrixName1)
    val genoSize = BlockPartitionMatrix.estimateBlockSizeWithDim(m1, n1)
    val geno = BlockPartitionMatrix.createFromCoordinateEntries(genoRDD, 2, 2, m1, n1)
    val I = new Array[BlockPartitionMatrix](3)
    for (i <- 0 until I.length) {
        //println(s"I($i)=")
        I(i) = genComponentOfI(geno, i)
        //println(I(i).toLocalMatrix())
    }
    val N = new Array[BlockPartitionMatrix](3)
    for (i <- 0 until N.length) {
        N(i) = I(i) %*% BlockPartitionMatrix.createVectorE(I(i))
        //println(s"N($i) = ")
        //println(N(i).toLocalMatrix())
    }
    val Si = new Array[BlockPartitionMatrix](3)
    for (i <- 0 until Si.length) {
        Si(i) = mrnaRank %*% (I(i).t)
        //println(s"Si($i) = ")
        //println(Si(i).toLocalMatrix())
    }
    val KK = geno.nCols()
    var S = (Si(0) ^ 2.0).divideVector(N(0))
    for (i <- 1 until 3) {
        if (N(i).nnz() != 0) {
            //println(s"i=$i" + "*"*20)
            S = S + (Si(i) ^ 2.0).divideVector(N(i))
        }
    }
    S = S * (12.0 / KK / (KK+1)) + (-3.0)*(KK+1)
    println(S.toLocalMatrix())
    Thread.sleep(10000)
  }

  def genComponentOfI(geno:BlockPartitionMatrix, v: Double): BlockPartitionMatrix = {
      val RDD = geno.blocks.map { case ((i, j), mat) =>
        ((i, j), LocalMatrix.matrixEquals(mat, v))
      }
      new BlockPartitionMatrix(RDD, geno.ROWS_PER_BLK, geno.COLS_PER_BLK, geno.nRows(), geno.nCols())
  }

  def genMrnaRDD(sc: SparkContext, name: String): RDD[Entry] = {
      val lines = sc.textFile(name, 8)
      lines.map { line =>
          if (line.contains("Sample")) {
              Array(Entry(-1, -1, -1))
          }
          else {
              val row = line.split("\t")(0).toLong - 1
              val res = RankData.rankWithNoMissing(line)
              val arr = new Array[Entry](res.length)
              for (j <- 0 until arr.length) {
                  arr(j) = Entry(row, j, res(j))
              }
              arr
          }
      }.flatMap(x => x)
      .filter(x => x.row >= 0)
  }

  def genGenoRDD(sc: SparkContext, name: String): RDD[Entry] = {
      val lines = sc.textFile(name, 8)
      lines.map { line =>
          if (line.contains("Sample")) {
              Array(Entry(-1, -1, -1))
          }
          else {
              val strArr = line.split("\t")
              val row = strArr(0).toLong - 1
              val arr = new Array[Entry](strArr.length-1)
              for (j <- 0 until arr.length) {
                  if (strArr(j+1).equals("NaN") || strArr(j+1).toInt == -1) {
                      // randomly assign for missing data
                      arr(j) = Entry(row, j, Random.nextInt(3))
                  }
                  else {
                      arr(j) = Entry(row, j, strArr(j + 1).toDouble)
                  }
              }
            arr
          }
      }.flatMap(x => x)
      .filter(x => x.row >= 0)
  }
}
