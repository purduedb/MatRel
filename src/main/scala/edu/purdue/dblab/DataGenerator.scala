package edu.purdue.dblab

import org.apache.spark.{SparkConf, SparkContext, Logging}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Created by yongyangyu on 10/13/15.
 */
class DataGenerator (
      val nrows: Long,
      val ncols: Long,
      val sparsity: Double,  // true: col skew; false: row skew
      val skewType: Boolean,
      val skew: Double) extends Logging{

      def genEntries(): ArrayBuffer[Entry] = {
          val nnz = nrows * ncols * sparsity
          val res = new ArrayBuffer[Entry]()
          var remains = nnz
          if (skewType) { // col skew
              val colCount = new ArrayBuffer[Long]()
              var cnt = 0L
              while (remains != 0) {
                  cnt += 1
                  require(cnt < ncols, s"There are still $remains entries left to be filled in!")
                  val curr = math.max(math.min((skew * remains).toLong, nrows), 1)
                  colCount.append(curr)
                  remains -= curr
              }
              for (j <- 0 until colCount.length) {
                  val rows = new mutable.HashSet[Long]()
                  while (rows.size < colCount(j)) {
                      rows.add(math.abs(Random.nextLong()) % nrows)
                  }
                  for (i <- rows) {
                      res.append(Entry(i, j, 1.0))
                  }
              }
              res
          }
          else {
              val rowCount = new ArrayBuffer[Long]()
              var cnt = 0L
              while (remains != 0) {
                  cnt += 1
                  require(cnt < nrows, s"There are still $remains entries left to be filled in!")
                  val curr = math.max(math.min((skew * remains).toLong, ncols), 1)
                  rowCount.append(curr)
                  remains -= curr
              }
              for (i <- 0 until rowCount.length) {
                  val cols = new mutable.HashSet[Long]()
                  while (cols.size < rowCount(i)) {
                      cols.add(math.abs(Random.nextLong()) % ncols)
                  }
                  for (j <- cols) {
                      res.append(Entry(i, j, 1.0))
                  }
              }
              res
          }
      }
}

object TestGenerator {
    def main (args: Array[String]){
        val data = new DataGenerator(100, 100, 0.1, true, 0.5)
        val conf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("Test for block partition matrices")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.consolidateFiles", "true")
        .set("spark.shuffle.compress", "false")
        val sc = new SparkContext(conf)
        val entryRDD = sc.parallelize(data.genEntries())
        val matrix = BlockPartitionMatrix.createFromCoordinateEntries(entryRDD, 50, 50, 100, 100)
        val collect = entryRDD.collect()
        for (elem <- collect) {
            println(elem)
        }
    }
}