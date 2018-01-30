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
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

object SparseMLlib {
  def main (args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: SparseMLlib <graphName>")
      System.exit(1)
    }
    val hdfs = "hdfs://172.18.11.128:8020/user/yu163/"
    val graphName = hdfs + "dataset/" + args(0)
    val conf = new SparkConf()
      .setAppName("Aggregation on sparse graphs")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.compress", "false")
      .set("spark.executor.cores", "10")
      .set("spark.cores.max", "50")
      .set("spark.executor.memory", "30g")
      .set("spark.default.parallelism", "300")
      .set("spark.rpc.message.maxSize", "1600")
    conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
    val sc = new SparkContext(conf)
    val coordinateRDD = genCoordinateRdd(sc, graphName)
    val dim = math.max(coordinateRDD.map(x => x.i).max, coordinateRDD.map(x => x.j).max) + 1
    val blkSize = 10000
    val coordinateMatrix = new CoordinateMatrix(coordinateRDD, dim, dim)
    val G = coordinateMatrix.toBlockMatrix(blkSize, blkSize)
    val gram_matrix = G.transpose.multiply(G)
    val vecRDD = sc.parallelize(0 until gram_matrix.numCols().toInt).map(x => MatrixEntry(x.toLong, 0, 1.0))
    val e = new CoordinateMatrix(vecRDD, dim, 1L).toBlockMatrix(blkSize, blkSize)
    gram_matrix.multiply(e).blocks.saveAsTextFile(hdfs + "tmp_result/aggregation")
    Thread.sleep(10000)
  }

  def genCoordinateRdd(sc: SparkContext, graphName: String): RDD[MatrixEntry] = {
    val lines = sc.textFile(graphName, 8)
    lines.map{ s =>
      val line = s.split("\\s+")
      if (line(0).charAt(0) == '#') {
        MatrixEntry(-1, -1, 0.0)
      } else {
        MatrixEntry(line(0).toLong, line(1).toLong, 1.0)
      }
    }.filter(x => x.i >= 0)
  }
}
