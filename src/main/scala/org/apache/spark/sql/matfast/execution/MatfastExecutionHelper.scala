package org.apache.spark.sql.matfast.execution

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.matfast.matrix.LocalMatrix
import org.apache.spark.sql.matfast.partitioner.{BlockCyclicPartitioner, ColumnPartitioner, RowPartitioner}
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
}
