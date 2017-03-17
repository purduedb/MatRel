package org.apache.spark.sql.matfast.execution

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.matfast.matrix.{LocalMatrix, MatrixBlock}
import org.apache.spark.sql.matfast.partitioner.{BlockCyclicPartitioner, ColumnPartitioner, IndexPartitioner, RowPartitioner}
import org.apache.spark.sql.matfast.util.MLMatrixSerializer

import scala.collection.concurrent.TrieMap

/**
  * Created by yongyangyu on 3/14/17.
  */
object MatfastExecutionHelper {

  def repartitionWithTargetPartitioner(partitioner: Partitioner,
    rdd: RDD[InternalRow]): RDD[((Int, Int), InternalRow)] = {
    partitioner match {
      case rowPart: RowPartitioner => RowPartitioner(rdd, rowPart.numPartitions)
      case colPart: ColumnPartitioner => ColumnPartitioner(rdd, colPart.numPartitions)
      case cyclicPart: BlockCyclicPartitioner => BlockCyclicPartitioner(rdd, cyclicPart.ROW_BLKS, cyclicPart.COL_BLKS,
        cyclicPart.ROW_BLKS_PER_PARTITION, cyclicPart.COL_BLKS_PER_PARTITION)
      case _ => throw new IllegalArgumentException(s"Partitioner not recognized for $partitioner")
    }
  }

  def genBlockCyclicPartitioner(nrows: Long, ncols: Long, blkSize: Int): (Int, Int, Int, Int) = {
    val ROW_BLK_NUM = math.ceil(nrows * 1.0 / blkSize).toInt
    val COL_BLK_NUM = math.ceil(ncols * 1.0 / blkSize).toInt
    val numPartitions = 64
    val scale = 1.0 / math.sqrt(numPartitions)
    var ROW_BLKS_PER_PARTITION = math.round(math.max(scale * ROW_BLK_NUM, 1.0)).toInt
    var COL_BLKS_PER_PARTITION = math.round(math.max(scale * COL_BLK_NUM, 1.0)).toInt
    if (ROW_BLKS_PER_PARTITION == 1 || COL_BLKS_PER_PARTITION == 1) {
      if (ROW_BLKS_PER_PARTITION != 1) {
        ROW_BLKS_PER_PARTITION = math.round(math.max(ROW_BLKS_PER_PARTITION / 8.0, 1.0)).toInt
      }
      if (COL_BLKS_PER_PARTITION != 1) {
        COL_BLKS_PER_PARTITION = math.round(math.max(COL_BLKS_PER_PARTITION / 8.0, 1.0)).toInt
      }
    }
    (ROW_BLK_NUM, COL_BLK_NUM, ROW_BLKS_PER_PARTITION, COL_BLKS_PER_PARTITION)
  }

  def addWithPartitioner(rdd1: RDD[((Int, Int), InternalRow)],
                         rdd2: RDD[((Int, Int), InternalRow)]): RDD[InternalRow] = {

    val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { (iter1, iter2) =>
      val buf = new TrieMap[(Int, Int), InternalRow]()
      for (a <- iter1) {
        if (a != null) {
          val idx = a._1
          if (!buf.contains(idx)) buf.putIfAbsent(idx, a._2)
          else {
            val old = buf.get(idx).get
            val res = LocalMatrix.add(MLMatrixSerializer.deserialize(old), MLMatrixSerializer.deserialize(a._2))
            buf.put(idx, MLMatrixSerializer.serialize(res))
          }
        }
      }
      for (b <- iter2) {
        if (b != null) {
          val idx = b._1
          if (!buf.contains(idx)) buf.putIfAbsent(idx, b._2)
          else {
            val old = buf.get(idx).get
            val res = LocalMatrix.add(MLMatrixSerializer.deserialize(old), MLMatrixSerializer.deserialize(b._2))
            buf.put(idx, MLMatrixSerializer.serialize(res))
          }
        }
      }
      buf.iterator
    }
    rdd.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrix)
      res
    }
  }

  def multiplyWithPartitioner(rdd1: RDD[((Int, Int), InternalRow)],
                         rdd2: RDD[((Int, Int), InternalRow)]): RDD[InternalRow] = {

    val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { case (iter1, iter2) =>
        val idx2val = new TrieMap[(Int, Int), InternalRow]()
        val res = new TrieMap[(Int, Int), InternalRow]()
        for (elem <- iter1) {
          val key = elem._1
          if (!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2)
        }
        for (elem <- iter2) {
          val key = elem._1
          if (idx2val.contains(key)) {
            val tmp = idx2val.get(key).get
            val product = MLMatrixSerializer.serialize(LocalMatrix.elementWiseMultiply(MLMatrixSerializer.deserialize(tmp),
              MLMatrixSerializer.deserialize(elem._2)))
            res.putIfAbsent(key, product)
          }
        }
        res.iterator
    }
    rdd.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrix)
      res
    }
  }

  def divideWithPartitioner(rdd1: RDD[((Int, Int), InternalRow)],
                            rdd2: RDD[((Int, Int), InternalRow)]): RDD[InternalRow] = {

    val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { case (iter1, iter2) =>
      val idx2val = new TrieMap[(Int, Int), InternalRow]()
      val res = new TrieMap[(Int, Int), InternalRow]()
      for (elem <- iter1) {
        val key = elem._1
        if (!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2)
      }
      for (elem <- iter2) {
        val key = elem._1
        if (idx2val.contains(key)) {
          val tmp = idx2val.get(key).get
          val division = MLMatrixSerializer.serialize(LocalMatrix.elementWiseDivide(MLMatrixSerializer.deserialize(tmp),
            MLMatrixSerializer.deserialize(elem._2)))
          res.putIfAbsent(key, division)
        }
      }
      res.iterator
    }
    rdd.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, matrix)
      res
    }
  }

  def multiplyOuterProductDuplicateLeft(rdd1: RDD[InternalRow], rdd2: RDD[InternalRow]): RDD[InternalRow] = {
    val numPartitions = rdd2.partitions.length
    val rightRDD = repartitionWithTargetPartitioner(new ColumnPartitioner(numPartitions), rdd2)
    val dupRDD = duplicateCrossPartitions(rdd1, numPartitions)
    dupRDD.zipPartitions(rightRDD, preservesPartitioning = true) { (iter1, iter2) =>
      val dup = iter1.next()._2
      for {
        x2 <- iter2
        x1 <- dup
      } yield ((x1.rid, x2._1._2), LocalMatrix.matrixMultiplication(x1.matrix, MLMatrixSerializer.deserialize(x2._2)))
    }.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(3, MLMatrixSerializer.serialize(matrix))
      res
    }
  }

  def multiplyOuterProductDuplicateRight(rdd1: RDD[InternalRow], rdd2: RDD[InternalRow]): RDD[InternalRow] = {
    val numPartitions = rdd1.partitions.length
    val leftRDD = repartitionWithTargetPartitioner(new RowPartitioner(numPartitions), rdd1)
    val dupRDD = duplicateCrossPartitions(rdd2, numPartitions)
    leftRDD.zipPartitions(dupRDD, preservesPartitioning = true) { (iter1, iter2) =>
      val dup = iter2.next()._2
      for {
        x1 <- iter1
        x2 <- dup
      } yield ((x1._1._1, x2.cid), LocalMatrix.matrixMultiplication(MLMatrixSerializer.deserialize(x1._2), x2.matrix))
    }.map { row =>
      val rid = row._1._1
      val cid = row._1._2
      val matrix = row._2
      val res = new GenericInternalRow(3)
      res.setInt(0, rid)
      res.setInt(1, cid)
      res.update(2, MLMatrixSerializer.serialize(matrix))
      res
    }
  }

  // duplicate the matrix for #partitions copies and distribute them over the cluster
  private def duplicateCrossPartitions(rdd: RDD[InternalRow], numPartitions: Int): RDD[(Int, Iterable[MatrixBlock])] = {
    rdd.flatMap { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrix = MLMatrixSerializer.deserialize(row.getStruct(2, 7))
      for (i <- 0 until numPartitions)
        yield (i, MatrixBlock(rid, cid, matrix))
    }.groupByKey(new IndexPartitioner(numPartitions))
  }

  def matrixMultiplyGeneral(rdd1: RDD[InternalRow], rdd2: RDD[InternalRow]): RDD[InternalRow] = {
    val leftRdd = rdd1.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      (cid, (rid, matrixInternalRow))
    }.groupByKey()
    val rightRdd = rdd2.map { row =>
      val rid = row.getInt(0)
      val cid = row.getInt(1)
      val matrixInternalRow = row.getStruct(2, 7)
      (rid, (cid, matrixInternalRow))
    }.groupByKey()
    leftRdd.join(rightRdd)
           .values
           .flatMap { case (iter1, iter2) =>
               for (blk1 <- iter1; blk2 <- iter2)
                 yield ((blk1._1, blk2._1),
                   LocalMatrix.matrixMultiplication(MLMatrixSerializer.deserialize(blk1._2),
                     MLMatrixSerializer.deserialize(blk2._2)))
           }.reduceByKey(LocalMatrix.add(_, _))
            .map { row =>
              val res = new GenericInternalRow(3)
              res.setInt(0, row._1._1)
              res.setInt(1, row._1._2)
              res.update(2, MLMatrixSerializer.serialize(row._2))
              res
            }
  }

  def matrixRankOneUpdate(rdd1: RDD[InternalRow], rdd2: RDD[InternalRow]): RDD[InternalRow] = {
    val numPartitions = rdd1.partitions.length
    val dupRdd = duplicateCrossPartitions(rdd2, numPartitions)
    rdd1.zipPartitions(dupRdd, preservesPartitioning = true) { (iter1, iter2) =>
      val dup = iter2.next()._2
      for {
        x1 <- iter1
        x2 <- dup
        x3 <- dup
        if (x1.getInt(0) == x2.rid && x1.getInt(1) == x3.rid)
      } yield (x1.getInt(0), x1.getInt(1),
        LocalMatrix.rankOneAdd(MLMatrixSerializer.deserialize(x1.getStruct(2, 7)), x2.matrix, x3.matrix))
    }.map { row =>
      val res = new GenericInternalRow(3)
      res.setInt(0, row._1)
      res.setInt(1, row._2)
      res.update(2, MLMatrixSerializer.serialize(row._3))
      res
    }
  }
}
