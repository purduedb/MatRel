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

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.sql.matfast.MatfastSession
import org.apache.spark.sql.matfast.matrix.{CooMatrix, MatrixBlock, RowMatrix}
import org.apache.spark.sql.matfast.partitioner.{BlockCyclicPartitioner, ColumnPartitioner, RowPartitioner}
import org.apache.spark.rdd.RDD

object MatrelJoin {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: MatrelJoin <matrix1> <matrix2>")
      System.exit(1)
    }
    val hdfs = "hdfs://172.18.11.128:8020/user/yu163/"
    val name1 = hdfs + "dataset/" + args(0)
    val name2 = hdfs + "dataset/" + args(1)
    val savePath = hdfs + "result/"
    val matfastSession = MatfastSession.builder()
      .appName("Matrel join values")
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
    runMatrixJoin(matfastSession, name1, name2, savePath)

    matfastSession.stop()
  }

  import scala.reflect.ClassTag
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  private def runMatrixJoin(spark: MatfastSession, name1: String, name2: String, savePath: String): Unit = {
    import spark.implicits._
    var dim1: Long = 0L
    var matrixRDD1: RDD[MatrixBlock] = null
    var isLeftSparse = false
    var isRightSparse = false
    if (name1.contains(".txt")) {
      val res = getSparseBlockMatrixRDD(spark, name1)
      dim1 = res._1
      matrixRDD1 = res._2
      isLeftSparse = true
    } else if (name1.contains(".csv")) {
      val res = getDenseBlockMatrixRDD(spark, name1)
      dim1 = res._1
      matrixRDD1 = res._2
    } else {
      throw new SparkException("unsupported file format")
    }
    var dim2: Long = 0L
    var matrixRDD2: RDD[MatrixBlock] = null
    if (name2.contains(".txt")) {
      val res = getSparseBlockMatrixRDD(spark, name2)
      dim2 = res._1
      matrixRDD2 = res._2
      isRightSparse = true
    } else if (name2.contains(".csv")) {
      val res = getDenseBlockMatrixRDD(spark, name2)
      dim2 = res._1
      matrixRDD2 = res._2
    } else {
      throw new SparkException("unsupported file format")
    }
    val mat1 = matrixRDD1.toDS()
    val mat2 = matrixRDD2.toDS()
    import spark.MatfastImplicits._
    //mat1.joinOnSingleIndex(dim1, dim1, isLeftSparse, mat2, dim2, dim2, isRightSparse, 3,
    //  (x: Double, y: Double) => x * y, 1000).rdd.saveAsTextFile(savePath)
    mat1.joinOnValues(dim1, dim1, isLeftSparse, mat2, dim2, dim2, isRightSparse,
      (x: Double, y: Double) => x * y, 500).rdd.saveAsTextFile(savePath)
    /*mat1.crossProduct(dim1, dim1, isLeftSparse,
      mat2, dim2, dim2, isRightSparse,
      (x: Double, y: Double) => x * y, 1000).rdd.saveAsTextFile(savePath)*/

  }

  def getSparseBlockMatrixRDD(spark: MatfastSession, graphname: String): (Long, RDD[MatrixBlock]) = {
    val lines = spark.sparkContext.textFile(graphname, 8)
    val entries = lines.map { s =>
      val line = s.split("\\s+")
      if (line(0).charAt(0) == '#') {
        MatrixEntry(-1, -1, 0.0)
      } else {
        MatrixEntry(line(0).toLong, line(1).toLong, 1.0)
      }
    }.filter(x => x.i >= 0)
    val dim = math.max(entries.map(x => x.i).max, entries.map(x => x.j).max) + 1
    val blkSize = 500
    val coordinateMatrix = new CooMatrix(entries, dim, dim)
    //val blk_num = math.ceil(dim * 1.0 / blkSize).toInt
    //val blkCyclic = new BlockCyclicPartitioner(blk_num, blk_num, blkSize, blkSize)
    //val blkRDD = coordinateMatrix.toBlockMatrixRDD(blkSize).partitionBy(blkCyclic)
    val blkRDD = coordinateMatrix.toBlockMatrixRDD(blkSize)//.partitionBy(new RowPartitioner(100))
    val rdd = blkRDD.map (x => MatrixBlock(x._1._1, x._1._2, x._2))
    (dim, rdd)
  }

  def getDenseBlockMatrixRDD(spark: MatfastSession, graphname: String): (Long, RDD[MatrixBlock]) = {
    val lines = spark.sparkContext.textFile(graphname, 8)
    val rowMat = new RowMatrix(lines)
    val blkSize = 500
    val dimRdd = rowMat.toBlockMatrixRDD(blkSize)
    val dim = dimRdd._1
    //val blk_num = math.ceil(dim * 1.0 / blkSize).toInt
    //val blkCyclic = new BlockCyclicPartitioner(blk_num, blk_num, blkSize, blkSize)
    //val rdd = dimRdd._2.partitionBy(blkCyclic)
    val rdd = dimRdd._2//.partitionBy(new RowPartitioner(100))
      .map(x => MatrixBlock(x._1._1, x._1._2, x._2))
    (dim, rdd)
  }
}
