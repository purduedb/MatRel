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
import org.apache.spark.sql.matfast.matrix._


object BasicMatrixOps {

  def main(args: Array[String]): Unit = {
    val matfastSession = MatfastSession.builder()
                                     .master("local[4]")
                                     .appName("SparkSessionForMatfast")
                                     .getOrCreate()
    // runMatrixTranspose(matfastSession)
    // runMatrixScalar(matfastSession)
    // runMatrixElement(matfastSession)
    // runMatrixMultiplication(matfastSession)
    // runMatrixAggregation(matfastSession)
    // runMatrixProjection(matfastSession)
    // runMatrixProjectCell(matfastSession)
    // runMatrixSelectValue(matfastSession)
    runMatrixCount(matfastSession)
    matfastSession.stop()
  }

  import scala.reflect.ClassTag
  // scalastyle:off
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)
  // scalastyle:on

  private def runMatrixTranspose(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 2, 2))
    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val b4 = new DenseMatrix(2, 2, Array[Double](4, 5, 6, 7))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))

    // val seq = Seq((0, 0, b1), (0, 1, b2), (1, 0, b3), (1, 1, b4))
    val seq = Seq(MatrixBlock(0, 2, s1), MatrixBlock(2, 3, b2), MatrixBlock(4, 5, b3), MatrixBlock(6, 7, b4)).toDS()
    import spark.MatfastImplicits._
    seq.t().rdd.foreach{ row =>
      // scalastyle:off
      println(row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }

  private def runMatrixScalar(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 2, 2))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))
    val seq = Seq(MatrixBlock(0, 2, b1), MatrixBlock(1, 3, s1)).toDS()
    import spark.MatfastImplicits._
    seq.power(2).rdd.foreach { row =>
      // scalastyle:off
      println(row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }

  private def runMatrixElement(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 2, 2))
    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))
    val seq1 = Seq(MatrixBlock(0, 0, b1), MatrixBlock(1, 1, b2)).toDS()
    val seq2 = Seq(MatrixBlock(0, 0, s1), MatrixBlock(0, 1, b3)).toDS()
    import spark.MatfastImplicits._
    seq1.addElement(4, 4, seq2, 4, 4, 2).rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":")
      println(row.get(2).asInstanceOf[MLMatrix])
    }
    println("-----------------")
    // scalastyle:on
    seq1.multiplyElement(4, 4, seq2, 4, 4, 2).rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":")
      println(row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }

  private def runMatrixMultiplication(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 2, 2))
    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val b4 = new DenseMatrix(2, 2, Array[Double](4, 5, 6, 7))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))
    val mat1 = Seq(MatrixBlock(0, 0, b1), MatrixBlock(1, 1, b2)).toDS()
    val mat2 = Seq(MatrixBlock(0, 0, b3), MatrixBlock(0, 1, b4), MatrixBlock(1, 1, s1)).toDS()
    import spark.MatfastImplicits._
    mat1.matrixMultiply(4, 4, mat2, 4, 4, 2).rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":")
      println(row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }

  /*
   * mat1 has the following structure
   * ---------------
   * | 1  2 |      |
   * | 1  2 |      |
   * ---------------
   * |      | 2  3 |
   * |      | 2  3 |
   * ---------------
   * and mat2 looks like the following
   * ---------------
   * | 3  4 | 4  6 |
   * | 3  4 | 5  7 |
   * ---------------
   * |      | 0  2 |
   * |      | 4  0 |
   * ---------------
   */

  private def runMatrixAggregation(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 2, 2))
    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val b4 = new DenseMatrix(2, 2, Array[Double](4, 5, 6, 7))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))
    val mat1 = Seq(MatrixBlock(0, 0, b1), MatrixBlock(1, 1, b2)).toDS()
    val mat2 = Seq(MatrixBlock(0, 0, b3), MatrixBlock(0, 1, b4), MatrixBlock(1, 1, s1)).toDS()

    import spark.MatfastImplicits._

    val mat1_rowsum = mat1.t().rowSum(4, 4)
    mat1_rowsum.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
    val mat2_colsum = mat2.colSum(4, 4)
    mat2_colsum.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }

    val product_trace = mat1.matrixMultiply(4, 4, mat2, 4, 4, 2).trace(4, 4)
    product_trace.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }

  private def runMatrixProjection(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 2, 2))
    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val b4 = new DenseMatrix(2, 2, Array[Double](4, 5, 6, 7))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))
    val mat1 = Seq(MatrixBlock(0, 0, b1), MatrixBlock(1, 1, b2)).toDS()
    val mat2 = Seq(MatrixBlock(0, 0, b3), MatrixBlock(0, 1, b4), MatrixBlock(1, 1, s1)).toDS()

    import spark.MatfastImplicits._

    val mat1_proj_row = mat1.projectRow(4, 4, 2, 2)
    mat1_proj_row.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }

    val mat2_proj_col = mat2.projectColumn(4, 4, 2, 3)
    mat2_proj_col.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }

    val mat2_X_mat2_col = mat1.matrixMultiply(4, 4, mat2, 4, 4, 2).projectColumn(4, 4, 2, 3)
    mat2_X_mat2_col.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }

  private def runMatrixProjectCell(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 2, 2))
    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val b4 = new DenseMatrix(2, 2, Array[Double](4, 5, 6, 7))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))
    val mat1 = Seq(MatrixBlock(0, 0, b1), MatrixBlock(1, 1, b2)).toDS()
    val mat2 = Seq(MatrixBlock(0, 0, b3), MatrixBlock(0, 1, b4), MatrixBlock(1, 1, s1)).toDS()

    import spark.MatfastImplicits._

    // select on the product of (mat1 X mat2)
    val mat_select = mat1.matrixMultiply(4, 4, mat2, 4, 4, 2).projectCell(4, 4, 2, 0, 3)
    mat_select.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }

  private def runMatrixSelectValue(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val b4 = new DenseMatrix(2, 2, Array[Double](4, 5, 6, 7))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))
    val mat1 = Seq(MatrixBlock(0, 0, b3), MatrixBlock(0, 1, b4), MatrixBlock(1, 1, s1)).toDS()
    import spark.MatfastImplicits._
    val mat_select_value = mat1.selectValue(5)
    mat_select_value.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
    /*
     * Test a new sparse matrix below
     *  --------------
     *  | 1  0  3  0 |
     *  | 0  5  0  0 |
     *  | 6  0  9  0 |
     *  | 0  8  0  3 |
     *  -------------
     */
    val s2 = new SparseMatrix(4, 4, Array[Int](0, 2, 4, 6, 7),
      Array[Int](0, 2, 1, 3, 0, 2, 3), Array[Double](1, 6, 5, 8, 3, 9, 3))
    val s3 = new SparseMatrix(4, 4, Array[Int](0, 2, 3, 5, 7),
      Array[Int](0, 2, 1, 0, 2, 1, 3), Array[Double](1, 3, 5, 6, 9, 8, 3), true)
    val mat2 = Seq(MatrixBlock(0, 0, s2)).toDS()
    val mat3 = Seq(MatrixBlock(0, 0, s3)).toDS()
    mat3.selectValue(3).removeEmptyColumns().rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }

  private def runMatrixCount(spark: MatfastSession): Unit = {
    import spark.implicits._
    import spark.MatfastImplicits._
    /*
     * Test a new sparse matrix below
     *  --------------
     *  | 1  0  3  0 |
     *  | 0  5  0  0 |
     *  | 6  0  9  0 |
     *  | 0  8  0  3 |
     *  -------------
     */
    val s2 = new SparseMatrix(4, 4, Array[Int](0, 2, 4, 6, 7),
      Array[Int](0, 2, 1, 3, 0, 2, 3), Array[Double](1, 6, 5, 8, 3, 9, 3))
    val s3 = new SparseMatrix(4, 4, Array[Int](0, 2, 3, 5, 7),
      Array[Int](0, 2, 1, 0, 2, 1, 3), Array[Double](1, 3, 5, 6, 9, 8, 3), true)
    val mat2 = Seq(MatrixBlock(0, 0, s2)).toDS()
    val mat3 = Seq(MatrixBlock(0, 0, s3)).toDS()
    mat3.nnz(4, 4).rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      // scalastyle:off
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
      // scalastyle:on
    }
  }
}