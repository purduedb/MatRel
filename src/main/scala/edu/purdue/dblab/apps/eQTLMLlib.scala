package edu.purdue.dblab.apps

import helper.RankData
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import org.apache.spark.mllib.linalg.{Matrix => SparkMatrix, DenseMatrix, SparseMatrix}
import org.apache.spark.{SparkException, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yongyangyu on 2/3/16.
  */
object eQTLMLlib {
    def main(args: Array[String]) {
        if (args.length < 6) {
            println("Usage: geno_matrix m1 n1 mrna_matrix m2 n2")
            System.exit(1)
        }
        val hdfs = "hdfs://10.100.121.126:8022/"
        val matrixName1 = hdfs + args(0)
        val (m1, n1) = (args(1).toLong, args(2).toLong)
        val matrixName2 = hdfs + args(3)
        val (m2, n2) = (args(4).toLong, args(5).toLong)
        val conf = new SparkConf()
          .setAppName("eQTL")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.shuffle.consolidateFiles", "true")
          .set("spark.shuffle.compress", "false")
          .set("spark.cores.max", "64")
          .set("spark.executor.memory", "58g")
          //.set("spark.default.parallelism", "64")
          .set("spark.akka.frameSize", "64")
        conf.setJars(SparkContext.jarOfClass(this.getClass).toArray)
        val sc = new SparkContext(conf)
        val mrnaSize = 2000  // set as fixed value of 2000 rows per block
        val mrnaRank = createDenseMatrix(sc, matrixName2, mrnaSize, mrnaSize,
                m2, n2, 64, RankData.rankWithNoMissing)
        mrnaRank.cache()
        val geno = createDenseMatrix(sc, matrixName1, mrnaSize, mrnaSize,
                m1, n1, 64, genoLine)
        geno.cache()
        val I = new Array[BlockMatrix](3)
        for (i <- 0 until I.length) {
            I(i) = genComponentI(geno, i).cache()
        }
        val N = new Array[BlockMatrix](3)
        for (i <- 0 until N.length) {
            N(i) = sumAlongRows(I(i)).cache()
        }
        val Si = new Array[BlockMatrix](3)
        for (i <- 0 until Si.length) {
            Si(i) = pow(mrnaRank.multiply(I(i).transpose), 2.0)
        }
        val KK = geno.numCols()
        var S = divideVector(Si(0), N(0))
        for (i <- 1 until 3) {
            S = S.add(divideVector(Si(i), N(i)))
        }
        S = scalaAdd(scalarMultiply(S, (12.0/KK/(KK+1))), -3.0*(KK+1))
        S.blocks.saveAsTextFile(hdfs + "tmp_result/eqtl")
        Thread.sleep(10000)
    }

    private def read(line: String): Array[Double] = {
        val elems = line.split("\t")
        val res = new Array[Double](elems.length)
        for (i <- 0 until res.length) {
            res(i) = elems(i).toDouble
        }
        res
    }

    def createDenseMatrix(sc: SparkContext,
                          name: String,
                          ROWS_PER_BLOCK: Int,
                          COLS_PER_BLOCK: Int,
                          nrows: Long,
                          ncols: Long,
                          npart: Int = 8,
                          procLine: String => Array[Double] = read): BlockMatrix = {
        val lines = sc.textFile(name, npart)
        val RDD0 = lines.map { line =>
            if (line.contains("#") || line.contains("Sample") || line.contains("HG") || line.contains("NA")) {
                (-1, (-1.toLong, line.toString))
            }
            else {
                val rowId = line.trim.split("\t")(0).toLong - 1
                ((rowId / ROWS_PER_BLOCK).toInt, (rowId.toLong, line.trim.substring((rowId+1).toString.length + 1)))
            }
        }.filter(x => x._1 >= 0)
        val RDD = RDD0.groupByKey()
                  .flatMap { case (rowBlkId, iter) =>
                      val numColBlks = math.ceil(ncols * 1.0/ COLS_PER_BLOCK).toInt
                      val arrs = Array.ofDim[Array[Double]](numColBlks)
                      var currRow = 0
                      if (rowBlkId == nrows / ROWS_PER_BLOCK) {
                          currRow = (nrows - ROWS_PER_BLOCK * rowBlkId).toInt
                      }
                      else {
                          currRow = ROWS_PER_BLOCK
                      }
                      for (j <- 0 until arrs.length) {
                          if (j == ncols / COLS_PER_BLOCK) {
                              arrs(j) = Array.ofDim[Double]((currRow*(ncols - ncols/COLS_PER_BLOCK*COLS_PER_BLOCK)).toInt)
                          }
                          else {
                              arrs(j) = Array.ofDim[Double](currRow * COLS_PER_BLOCK)
                          }
                      }
                      for (row <- iter) {
                          val rowId = row._1
                          val values = procLine(row._2)
                          for (j <- 0 until values.length) {
                              val colBlkId = j / COLS_PER_BLOCK
                              val localRowId = rowId - rowBlkId * ROWS_PER_BLOCK
                              val localColId = j - colBlkId * COLS_PER_BLOCK
                              val idx = currRow * localColId + localRowId
                              arrs(colBlkId)(idx.toInt) = values(j)
                          }
                      }
                      val buffer = ArrayBuffer[((Int, Int), SparkMatrix)]()
                      for (j <- 0 until arrs.length) {
                          if (j == ncols / COLS_PER_BLOCK) {
                              buffer.append(((rowBlkId.toInt, j),
                                new DenseMatrix(currRow, (ncols - ncols/COLS_PER_BLOCK*COLS_PER_BLOCK).toInt, arrs(j))))
                          }
                          else {
                              buffer.append(((rowBlkId.toInt, j),
                                new DenseMatrix(currRow, COLS_PER_BLOCK, arrs(j))))
                          }
                      }
                      buffer
                  }
        new BlockMatrix(RDD, ROWS_PER_BLOCK, COLS_PER_BLOCK, nrows, ncols)
    }

    def genoLine(line: String): Array[Double] = {
        val elems = line.split("\t")
        val res = new Array[Double](elems.length)
        for (i <- 0 until res.length) {
            if (elems(i).equals("NaN") || elems(i).toInt == -1) {
                res(i) = 0
            }
            else {
                res(i) = elems(i).toDouble
            }
        }
        res
    }

    def matrixEquals(mat: SparkMatrix, v: Double): SparkMatrix = {
        mat match {
            case den: DenseMatrix =>
                val values = den.toArray
                for (i <- 0 until values.length) {
                    if (math.abs(v - values(i)) < 1e-6) {
                        values(i) = 1
                    }
                    else {
                        values(i) = 0
                    }
                }
                new DenseMatrix(mat.numRows, mat.numCols, values)
            case sp: SparseMatrix =>
                val values = sp.toArray
                for (i <- 0 until values.length) {
                    if (math.abs(v - values(i)) < 1e-6) {
                        values(i) = 1
                    }
                    else {
                        values(i) = 0
                    }
                }
                new DenseMatrix(mat.numRows, mat.numCols, values)
        }
    }

    def genComponentI(geno: BlockMatrix, v: Double): BlockMatrix = {
        val RDD = geno.blocks.map { case ((i, j), mat) =>
            ((i, j), matrixEquals(mat, v))
        }
        new BlockMatrix(RDD, geno.rowsPerBlock, geno.colsPerBlock, geno.numRows(), geno.numCols())
    }

    def denseAdd(mat1: SparkMatrix, mat2: SparkMatrix): SparkMatrix = {
        val result = (mat1, mat2) match {
            case (den1: DenseMatrix, den2: DenseMatrix) =>
                val v1 = den1.values
                val v2 = den2.values
                val values = new Array[Double](v1.length)
                for (i <- 0 until values.length) {
                    values(i) += v1(i) + v2(i)
                }
                new DenseMatrix(den1.numRows, den1.numCols, values)
            case _ => throw new SparkException("Undefined matrix type for sumAlongRows()")
        }
        result
    }

    def sumAlongRows(mat: BlockMatrix): BlockMatrix = {
        val rdd = mat.blocks.map { case ((rowIdx, colIdx), mat) =>
            val matrix = mat match {
                case den: DenseMatrix =>
                    val arr = new Array[Double](den.numRows)
                    val m = den.numRows
                    val values = den.values
                    for (i <- 0 until values.length) {
                        arr(i % m) += values(i)
                    }
                    new DenseMatrix(m, 1, arr)
                case sp: SparseMatrix =>
                    val arr = new Array[Double](sp.numRows)
                    val m = sp.numRows
                    val values = sp.toArray
                    for (i <- 0 until values.length) {
                        arr(i % m) += values(i)
                    }
                    new DenseMatrix(m, 1, arr)
                case _ => throw new SparkException("Undefined matrix type")
            }
            (rowIdx, matrix.asInstanceOf[SparkMatrix])
        }.reduceByKey(denseAdd(_, _))
         .map { case (rowIdx, mat) =>
             ((rowIdx, 0), mat)
         }
        new BlockMatrix(rdd, mat.rowsPerBlock, mat.colsPerBlock, mat.numRows(), 1L)
    }

    def pow(mat: BlockMatrix, p: Double): BlockMatrix = {
        val rdd = mat.blocks.map { case ((i, j), matrix) =>
            val res = matrix match {
                case den: DenseMatrix =>
                    val values = den.values.clone()
                    for (i <- 0 until values.length) {
                        values(i) = math.pow(values(i), p)
                    }
                    new DenseMatrix(den.numRows, den.numCols, values, den.isTransposed)
                case sp: SparseMatrix =>
                    val values = sp.values.clone()
                    for (i <- 0 until values.length) {
                        values(i) = math.pow(values(i), p)
                    }
                    new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, values, sp.isTransposed)
                case _ => throw new SparkException("Undefined matrix type in pow()")
            }
            ((i, j), res)
        }
        new BlockMatrix(rdd, mat.rowsPerBlock, mat.colsPerBlock, mat.numRows(), mat.numCols())
    }

    def localDivideVector(mat: SparkMatrix, vec: SparkMatrix): SparkMatrix = {
        require(mat.numCols == vec.numRows, s"Dimension error for matrix divide vector, " +
        s"mat.numCols=${mat.numCols}, vec.numRows=${vec.numRows}")
        mat match {
            case dm: DenseMatrix =>
                val arr = dm.values.clone()
                val div = vec.toArray
                val n = dm.numRows
                for (i <- 0 until arr.length) {
                    if (div(i / n) != 0) {
                        arr(i) = arr(i) / div(i/n)
                    }
                    else {
                        arr(i) = 0.0
                    }
                }
                new DenseMatrix(mat.numRows, mat.numCols, arr)
            case sp: SparseMatrix =>
                val arr = sp.values.clone()
                val div = vec.toArray
                val rowIdx = sp.rowIndices
                val colPtr = sp.colPtrs
                for (cid <- 0 until colPtr.length-1) {
                    val count = colPtr(cid+1) - colPtr(cid)
                    for (i <- 0 until count) {
                        val idx = colPtr(cid) + 1
                        if (div(cid) != 0) {
                            arr(idx) = arr(idx) / div(cid)
                        }
                        else {
                            arr(idx) = 0.0
                        }
                    }
                }
                new SparseMatrix(sp.numRows, sp.numCols, colPtr, rowIdx, arr)
            case _ => throw new SparkException("Local matrix format not recognized!")
        }
    }

    def divideVector(mat: BlockMatrix, vec: BlockMatrix): BlockMatrix = {
        val RDD1 = mat.blocks.map { case ((i, j), mat) =>
            (j, ((i, j), mat))
        }
        val RDD2 = vec.blocks.map { case ((i, j), mat) =>
            (i, mat)
        }
        val RDD = RDD1.join(RDD2).map { case (idx, (((i,j), mat1), mat2)) =>
            ((i, j), localDivideVector(mat1, mat2))
        }
        new BlockMatrix(RDD, mat.rowsPerBlock, mat.colsPerBlock, mat.numRows(), mat.numCols())
    }

    def scalarMultiply(mat: BlockMatrix, q: Double): BlockMatrix = {
        val rdd = mat.blocks.map { case ((i, j), mat) =>
            val res = mat match {
                case dm: DenseMatrix =>
                    val values = dm.values.clone()
                    for (i <- 0 until values.length) {
                        values(i) = values(i) * q
                    }
                    new DenseMatrix(dm.numRows, dm.numCols, values, dm.isTransposed)
                case sp: SparseMatrix =>
                    val values = sp.values.clone()
                    for (i <- 0 until values.length) {
                        values(i) = values(i) * q
                    }
                    new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, values, sp.isTransposed)
            }
            ((i, j), mat)
        }
        new BlockMatrix(rdd, mat.rowsPerBlock, mat.colsPerBlock, mat.numRows(), mat.numCols())
    }

    def scalaAdd(mat: BlockMatrix, q: Double): BlockMatrix = {
        val rdd = mat.blocks.map { case ((i, j), mat) =>
            val res = mat match {
                case dm: DenseMatrix =>
                    val values = dm.values.clone()
                    for (i <- 0 until values.length) {
                        values(i) += q
                    }
                    new DenseMatrix(dm.numRows, dm.numCols, values, dm.isTransposed)
                case sp: SparseMatrix =>
                    val values = sp.values.clone()
                    for (i <- 0 until values.length) {
                        values(i) += q
                    }
                    new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, values, sp.isTransposed)
            }
            ((i, j), mat)
        }
        new BlockMatrix(rdd, mat.rowsPerBlock, mat.colsPerBlock, mat.numRows(), mat.numCols())
    }
}
