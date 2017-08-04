package org.apache.spark.sql.matfast.example

import org.apache.spark.sql.matfast.MatfastSession
import org.apache.spark.sql.matfast.matrix._


/**
  * Created by yongyangyu on 2/21/17.
  */
object BasicMatrixOps {

  def main(args: Array[String]): Unit = {
    val matfastSession = MatfastSession.builder()
                                     .master("local[4]")
                                     .appName("SparkSessionForMatfast")
                                     .getOrCreate()
    //runMatrixTranspose(matfastSession)
    //runMatrixScalar(matfastSession)
    //runMatrixElement(matfastSession)
    //runMatrixMultiplication(matfastSession)
    runMatrixAggregation(matfastSession)
    matfastSession.stop()
  }

  import scala.reflect.ClassTag
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  private def runMatrixTranspose(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2,2,Array[Double](1,1,2,2))
    val b2 = new DenseMatrix(2,2,Array[Double](2,2,3,3))
    val b3 = new DenseMatrix(2,2,Array[Double](3,3,4,4))
    val b4 = new DenseMatrix(2,2,Array[Double](4,5,6,7))
    val s1 = new SparseMatrix(2,2,Array[Int](0,1,2),Array[Int](1,0),Array[Double](4,2))

    //val seq = Seq((0, 0, b1), (0, 1, b2), (1, 0, b3), (1, 1, b4))
    val seq = Seq(MatrixBlock(0, 2, s1), MatrixBlock(2, 3, b2), MatrixBlock(4, 5, b3), MatrixBlock(6, 7, b4)).toDS()
    import spark.MatfastImplicits._
    seq.t().rdd.foreach{ row =>
      println(row.get(2).asInstanceOf[MLMatrix])
    }
  }

  private def runMatrixScalar(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2,2,Array[Double](1,1,2,2))
    val s1 = new SparseMatrix(2,2,Array[Int](0,1,2),Array[Int](1,0),Array[Double](4,2))
    val seq = Seq(MatrixBlock(0, 2, b1), MatrixBlock(1, 3, s1)).toDS()
    import spark.MatfastImplicits._
    seq.power(2).rdd.foreach { row =>
      println(row.get(2).asInstanceOf[MLMatrix])
    }
  }

  private def runMatrixElement(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2,2,Array[Double](1,1,2,2))
    val b2 = new DenseMatrix(2,2,Array[Double](2,2,3,3))
    val b3 = new DenseMatrix(2,2,Array[Double](3,3,4,4))
    val s1 = new SparseMatrix(2,2,Array[Int](0,1,2),Array[Int](1,0),Array[Double](4,2))
    val seq1 = Seq(MatrixBlock(0, 0, b1), MatrixBlock(1, 1, b2)).toDS()
    val seq2 = Seq(MatrixBlock(0, 0, s1), MatrixBlock(0, 1, b3)).toDS()
    import spark.MatfastImplicits._
    seq1.addElement(4, 4, seq2.toDF(), 4, 4, 2).rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      println(idx + ":")
      println(row.get(2).asInstanceOf[MLMatrix])
    }
    println("-----------------")
    seq1.multiplyElement(4, 4, seq2.toDF(), 4, 4, 2).rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      println(idx + ":")
      println(row.get(2).asInstanceOf[MLMatrix])
    }
  }

  private def runMatrixMultiplication(spark: MatfastSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2,2,Array[Double](1,1,2,2))
    val b2 = new DenseMatrix(2,2,Array[Double](2,2,3,3))
    val b3 = new DenseMatrix(2,2,Array[Double](3,3,4,4))
    val b4 = new DenseMatrix(2,2,Array[Double](4,5,6,7))
    val s1 = new SparseMatrix(2,2,Array[Int](0,1,2),Array[Int](1,0),Array[Double](4,2))
    val mat1 = Seq(MatrixBlock(0,0,b1), MatrixBlock(1,1,b2)).toDS()
    val mat2 = Seq(MatrixBlock(0,0,b3), MatrixBlock(0,1,b4), MatrixBlock(1,1,s1)).toDS()
    import spark.MatfastImplicits._
    mat1.matrixMultiply(4, 4, mat2.toDF(), 4, 4, 2).rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      println(idx + ":")
      println(row.get(2).asInstanceOf[MLMatrix])
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
    val b1 = new DenseMatrix(2,2,Array[Double](1,1,2,2))
    val b2 = new DenseMatrix(2,2,Array[Double](2,2,3,3))
    val b3 = new DenseMatrix(2,2,Array[Double](3,3,4,4))
    val b4 = new DenseMatrix(2,2,Array[Double](4,5,6,7))
    val s1 = new SparseMatrix(2,2,Array[Int](0,1,2),Array[Int](1,0),Array[Double](4,2))
    val mat1 = Seq(MatrixBlock(0,0,b1), MatrixBlock(1,1,b2)).toDS()
    val mat2 = Seq(MatrixBlock(0,0,b3), MatrixBlock(0,1,b4), MatrixBlock(1,1,s1)).toDS()
    import spark.MatfastImplicits._
    val mat1_rowsum = mat1.t().rowSum(4, 4)
    mat1_rowsum.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
    }
    val mat2_colsum = mat2.colSum(4, 4)
    mat2_colsum.rdd.foreach { row =>
      val idx = (row.getInt(0), row.getInt(1))
      println(idx + ":\n" + row.get(2).asInstanceOf[MLMatrix])
    }
  }
}