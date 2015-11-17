package edu.purdue.dblab.matrix

import org.apache.spark.Partitioner
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by yongyangyu on 6/26/15.
 * MatrixRangePartitioner partitions the row/col matrix into equal width of rows/cols
 */
class MatrixRangePartitioner(partitions: Int, num: Long) extends Partitioner {
    require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions")

    def numPartitions = partitions


    def getPartition(key: Any): Int = key match {
        case null => 0

        case rcid: Long => {
            val partitionSize = num / partitions
            require(partitionSize != 0, s"partitionSize = 0, numRows = $num, partitions = $partitions")
            (rcid / partitionSize).toInt
        }
    }

    override def equals(other: Any): Boolean = {
        other.isInstanceOf[MatrixRangePartitioner] &&
        numPartitions == other.asInstanceOf[MatrixRangePartitioner].numPartitions
    }
}

/*
 *  MatrixRangeGreedyPartitioner implements greedy row/column partitioning scheme.
 *  Each partition is assigned approximately the same number of entries (not vectors),
 *  which provides load balancing property.
 */
class MatrixRangeGreedyPartitioner(partitions: Int, rdd: RDD[(Long, Vector)]) extends Partitioner {
    require(partitions > 0, s"Number of partitions must be positive but found $partitions")

    def numPartitions = partAssginment.size

    def getPartition(key: Any): Int = key match {
        case null => 0

        case rcid: Long => {
            for (key <- partAssginment.keySet) {
                if (partAssginment.get(key).exists(h => h.contains(rcid))) {
                    return key
                }
            }
            numPartitions - 1
        }
    }

    override def equals(other: Any): Boolean = {
        other.isInstanceOf[MatrixRangeGreedyPartitioner] &&
        numPartitions == other.asInstanceOf[MatrixRangeGreedyPartitioner].numPartitions
    }

    private val rowSizeRdd = rdd.map(row => (row._1, row._2.size))

    private val partitionSize = {
        val totalSize = rowSizeRdd.map(row => row._2).aggregate(0L)(_ + _, _ + _)
        totalSize / partitions  // floor of the avg
    }

    private val partAssginment = {
        val arrIter = (iter: Iterator[(Long, Int)]) => {
            val elem = iter.toArray
            Iterator(elem)
        }
        val all = rowSizeRdd.mapPartitions(arrIter).collect()  // Array[Array[(Long, Int)]]
        var partitionMap = scala.collection.mutable.HashMap[Int, scala.collection.mutable.HashSet[Long]]()
        var (parId, capacity) = (0, 0L)
        var holder = new scala.collection.mutable.HashSet[Long]()
        for (i <- 0 until all.length) {
            for (j <- 0 until all(i).length) {
                if (capacity + all(i)(j)._2 <= partitionSize) {
                    capacity += all(i)(j)._2
                    holder += all(i)(j)._1
                }
                else {
                    if (!holder.isEmpty) {
                        partitionMap += (parId -> holder)
                        parId += 1
                    }
                    holder = new mutable.HashSet[Long]()
                    holder += all(i)(j)._1
                    capacity = all(i)(j)._2
                }
            }
        }
        partitionMap += (parId -> holder)
        partitionMap
    }
}

/*
 * To make load-balancing, we adopt block-cyclic distribution strategy.
 * For further information, please refer to Section 1.6 from "Matrix Computation" (4th edition)
 * by Gene Golub and Charles Van Loan.
 */
class BlockCyclicPartitioner(
          val ROW_BLKS: Int,
          val COL_BLKS: Int,
          val ROW_BLKS_PER_PARTITION: Int,
          val COL_BLKS_PER_PARTITION: Int) extends Partitioner {
    require(ROW_BLKS > 0, s"Number of row blocks should be larger than 0, but found $ROW_BLKS")
    require(COL_BLKS > 0, s"Number of col blocks should be larger than 0, but found $COL_BLKS")
    require(ROW_BLKS_PER_PARTITION > 0, s"Number of row blocks per partition should be larger than 0, " +
            s"but found $ROW_BLKS_PER_PARTITION")
    require(COL_BLKS_PER_PARTITION > 0, s"Number of col blocks per partition should be larger than 0, " +
            s"but found $COL_BLKS_PER_PARTITION")

    private val row_partition_num = math.ceil(ROW_BLKS * 1.0 / ROW_BLKS_PER_PARTITION).toInt
    private val col_partition_num = math.ceil(COL_BLKS * 1.0 / COL_BLKS_PER_PARTITION).toInt

    private val num_row_part = ROW_BLKS / row_partition_num
    private val num_col_part = COL_BLKS / col_partition_num

    //println(s"num_row_part = $num_row_part")
    //println(s"num_col_part = $num_col_part")

    override val numPartitions: Int = row_partition_num * col_partition_num

    override def getPartition(key: Any): Int = {
        key match {
            case (i: Int, j : Int) => ((i % num_row_part) * col_partition_num + (j % num_col_part)) % numPartitions
            case (i: Int, j: Int, _: Int) => ((i % num_row_part) * col_partition_num + (j % num_col_part)) % numPartitions
            case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
        }
    }

    override def equals(obj: Any): Boolean = {
        obj match {
            case r: BlockCyclicPartitioner =>
                (ROW_BLKS == r.ROW_BLKS) &&
                (COL_BLKS == r.COL_BLKS) &&
                (ROW_BLKS_PER_PARTITION == r.ROW_BLKS_PER_PARTITION) &&
                (COL_BLKS_PER_PARTITION == r.COL_BLKS_PER_PARTITION)
            case _ => false
        }
    }

    override def hashCode: Int = {
        com.google.common.base.Objects.hashCode(
            ROW_BLKS: java.lang.Integer,
            COL_BLKS: java.lang.Integer,
            ROW_BLKS_PER_PARTITION: java.lang.Integer,
            COL_BLKS_PER_PARTITION: java.lang.Integer
        )
    }
}

class RowPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions")

    def numPartitions = partitions

    def getPartition(key: Any): Int = {
        key match {
            case (i: Int, j: Int) => i % partitions
            case (i: Int, j: Int, _: Int) => i % partitions
            case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
        }
    }

    override def equals(other: Any): Boolean = {
        other.isInstanceOf[RowPartitioner] &&
          numPartitions == other.asInstanceOf[RowPartitioner].numPartitions
    }
}

class ColumnPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions")

    def numPartitions = partitions

    def getPartition(key: Any): Int = {
        key match {
            case (i: Int, j: Int) => j % partitions
            case (i: Int, j: Int, _: Int) => j % partitions
            case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
        }
    }

    override def equals(other: Any): Boolean = {
        other.isInstanceOf[ColumnPartitioner] &&
          numPartitions == other.asInstanceOf[ColumnPartitioner].numPartitions
    }
}

class IndexPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions")

    def numPartitions = partitions

    def getPartition(key: Any): Int = {
        key match {
            case (i: Int) => i
            case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
        }
    }

    override def equals(other: Any): Boolean = {
        other.isInstanceOf[IndexPartitioner] &&
          numPartitions == other.asInstanceOf[IndexPartitioner].numPartitions
    }
}

/*
 *  provides some factory methods for creating BlockCyclicPartitioner instances.
 */

object BlockCyclicPartitioner {
    def apply(ROW_BLKS: Int, COL_BLKS: Int,
              ROW_BLKS_PER_PARTITION: Int, COL_BLKS_PER_PARTITION: Int): BlockCyclicPartitioner = {
        new BlockCyclicPartitioner(ROW_BLKS, COL_BLKS, ROW_BLKS_PER_PARTITION, COL_BLKS_PER_PARTITION)
    }

    def apply(ROW_BLKS: Int, COL_BLKS: Int, suggestedNumPartitions: Int): BlockCyclicPartitioner = {
        require(suggestedNumPartitions > 0, s"Number of partitions should be larger than 0, found " +
        s"suggested number = $suggestedNumPartitions")
        val scale = 1.0 / math.sqrt(suggestedNumPartitions)
        val ROW_BLKS_PER_PARTITION = math.round(math.max(scale * ROW_BLKS, 1.0)).toInt
        val COL_BLKS_PER_PARTITION = math.round(math.max(scale * COL_BLKS, 1.0)).toInt
        new BlockCyclicPartitioner(ROW_BLKS, COL_BLKS, ROW_BLKS_PER_PARTITION, COL_BLKS_PER_PARTITION)
    }

}