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

import org.apache.spark.sql.matfast.MatfastSession
import org.apache.spark.sql.matfast.matrix.{MatrixBlock, RowMatrix}
import org.apache.spark.sql.matfast.partitioner.RowPartitioner
import org.apache.spark.rdd.RDD

object MatrelAggDense {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: SparseMLlib <graphName>")
      System.exit(1)
    }
    val hdfs = "hdfs://172.18.11.128:8020/user/yu163/"
    val graphName = hdfs + "dataset/" + args(0)
    val savePath = hdfs + "result/"
    val matfastSession = MatfastSession.builder()
      .appName("Matrel agg on mat-mat multiply dense")
      .master("spark://172.18.11.128:7077")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.consolidateFiles", "true")
      .config("spark.shuffle.compress", "false")
      .config("spark.executor.cores", "16")
      .config("spark.cores.max", "80")
      .config("spark.executor.memory", "24g")
      .config("spark.default.parallelism", "200")
      .config("spark.rpc.message.maxSize", "1000")
      .getOrCreate()
    runAggOnMatrixMultiply(matfastSession, graphName, savePath)

    matfastSession.stop()
  }

  import scala.reflect.ClassTag
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  private def runAggOnMatrixMultiply(spark: MatfastSession, graphname: String, savePath: String): Unit = {
    import spark.implicits._
    val (dim, matrixRDD) = getBlockMatrixRDD(spark, graphname)
    val matrix = matrixRDD.toDS()
    import spark.MatfastImplicits._
    val G = matrix.t().matrixMultiply(dim, dim, matrix, dim, dim, 1000)
      G.rdd.count()
      G.projectCell(dim, dim, 1000, 1, 1).rdd.saveAsTextFile(savePath)
    //matrix.t().matrixMultiply(dim, dim, matrix, dim, dim, 1000).trace(dim, dim).rdd.saveAsTextFile(savePath)
    //val GG = matrix.t().matrixMultiply(dim, dim, matrix, dim, dim, 1000)
    //GG.rdd.count()
    //GG.trace(dim, dim).rdd.saveAsTextFile(savePath)
  }

  def getBlockMatrixRDD(spark: MatfastSession, graphname: String): (Long, RDD[MatrixBlock]) = {
    val lines = spark.sparkContext.textFile(graphname, 8)
    val rowMat = new RowMatrix(lines)
    val blkSize = 1000
    val dimRdd = rowMat.toBlockMatrixRDD(blkSize)
    val dim = dimRdd._1
    val rdd = dimRdd._2.partitionBy(new RowPartitioner(100))
      .map(x => MatrixBlock(x._1._1, x._1._2, x._2))
    (dim, rdd)
  }
}
