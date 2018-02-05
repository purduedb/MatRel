/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.matfast.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.Vector

object DenseMLlib {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: DenseMLlib <graphName>")
      System.exit(1)
    }
    val hdfs = "hdfs://172.18.11.128:8020/user/yu163/"
    val graphName = hdfs + "dataset/" + args(0)
    val conf = new SparkConf()
      .setAppName("Aggregation on dense graphs")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.compress", "false")
      .set("spark.executor.cores", "16")
      .set("spark.cores.max", "80")
      .set("spark.executor.memory", "24g")
      .set("spark.default.parallelism", "300")
      .set("spark.rpc.message.maxSize", "1600")
      .set("spark.kryoserializer.buffer.max", "256m")
    conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
    val sc = new SparkContext(conf)
    val blkSize = 1000
    val matrix = genIndexedRowMatrix(sc, graphName).toBlockMatrix(blkSize, blkSize)
    val dim = matrix.numCols()
    println(s"dim = $dim")
    val gram_matrix = matrix.transpose.multiply(matrix)
    //val vecRDD = sc.parallelize(0 until gram_matrix.numCols().toInt).map(x => MatrixEntry(x.toLong, 0, 1.0))
    //val e = new CoordinateMatrix(vecRDD, dim, 1L).toBlockMatrix(blkSize, blkSize)
    //gram_matrix.multiply(e).blocks.saveAsTextFile(hdfs + "tmp_result/aggregation")
    gram_matrix.blocks.filter(x => x._1 == (0, 0)).saveAsTextFile(hdfs + "tmp_result/aggregation")
    /*val trace = gram_matrix.blocks
      .map { case ((rid, cid), mat) =>
        if (rid != cid) {
          0.0
        } else {
          val nrows = mat.numRows
          var diag = 0.0
          for (i <- 0 until nrows) {
            diag += mat(i, i)
          }
          diag
        }
      }.reduce(_ + _)
    println(s"trace = $trace")*/
    Thread.sleep(10000)
  }

  def genIndexedRowMatrix(sc: SparkContext, graphName: String): IndexedRowMatrix = {
    val lines = sc.textFile(graphName, 8)
    val rdd = lines.map { s =>
      val line = s.trim().split(",")
      val idx = line(0).toLong
      val arr = new Array[Double](line.length - 1)
      for (i <- 0 until arr.length) {
        arr(i) = line(i + 1).toDouble
      }
      IndexedRow(idx, new DenseVector(arr))
    }
    val idxRowMat = new IndexedRowMatrix(rdd)
    idxRowMat
  }

  def getRowMatrix(sc: SparkContext, graphName: String): RowMatrix = {
    val lines = sc.textFile(graphName, 8)
    val rdd = lines.map { s =>
      val line = s.trim().split(",")
      val arr = new Array[Double](line.length)
      for (i <- 0 until arr.length) {
        arr(i) = line(i).toDouble
      }
      (new DenseVector(arr)).asInstanceOf[Vector]
    }
    new RowMatrix(rdd)
  }
}