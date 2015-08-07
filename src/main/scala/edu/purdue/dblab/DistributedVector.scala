package edu.purdue.dblab

/**
 * Created by yongyangyu on 6/17/15.
 * Distributed vector
 * We do not distinguish the concept of row vector or column vector.
 * As long as the dimension of two vector are compatible, they perform any binary operations between
 * themselves.
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg._
import org.apache.spark.RangePartitioner


case class dvEntry(idx: Long, v: Double) {
    override def toString() = s"($idx, $v)"
}
// each entry is a tuple of the format (idx, value)
class DistributedVector(val entries: RDD[dvEntry],
                        private var _size: Long) extends Logging {
    /*
     * size is the dimension of the distributed vector
     * if the _size field is set, the member value will be returned
     * otherwise, the size will be computed based on each partition
     */
    def size: Long = {
        if (_size <= 0) {
            _size = entries.map(entry => entry.idx).max() + 1
        }
        _size
    }
    /*
     * nnz is the number of nonzero elements in the distributed vector
     */
    def nnz: Long = {
        entries.count()
    }

  def apply(i: Long): Double = {
      require(i < size, s"index out of bound, size = $size, i = $i")
      entries.filter(e => e.idx == i).collect()(0).v
  }
  /*
   * norm() computes the p-norm of the distributed vector
   */
  def norm(p: Double): Double = {
      if (p == 1) {
        entries.map(entry => math.abs(entry.v)).aggregate(0.0)(_ + _, _ + _)
      }
      else if (p == 2){
        val sq = entries.map(entry => entry.v * entry.v).aggregate(0.0)(_ + _, _ + _)
        math.sqrt(sq)
      }
      else if (p == Double.PositiveInfinity){
        entries.map(entry => math.abs(entry.v)).max()
      }
      else {
        val sq = entries.map(entry => math.pow(entry.v, p)).aggregate(0.0)(_ + _, _ + _)
        math.pow(sq, 1.0 / p)
      }
  }
  /*
   * inner() computes the inner-product of two distributed vectors
   */
  def inner(other: DistributedVector): Double = {
      val s1 = size
      val s2 = other.size
      require(s1 == s2, s"Requested inner product but got vector1 with size $s1 and vector2 with size $s2")
      val currRdd = entries.map(entry => (entry.idx, entry.v))
      val otherRdd = other.entries.map(entry => (entry.idx, entry.v))
      currRdd.join(otherRdd).map(x => x._2._1 * x._2._1).sum()
  }
  def inner(other: Vector): Double = {
      val s1 = size
      val s2 = other.size
      require(s1 == s2, s"Requested inner product but got vector1 with size $s1 and vector2 with size $s2")
      val partitionFunc = (iter: Iterator[dvEntry]) => {
          iter.map { x =>
              val (idx, v) = (x.idx, x.v)
              val sparsify = other.toSparse
              val indices = sparsify.indices
              val vs = sparsify.values
              val find = indices.indexWhere(x => x == idx)
              if (find < 0) 0 else v * vs(find)
          }
      }
      entries.mapPartitions(partitionFunc).sum()
  }
  /*
   * multiply(alpha: Double) computes the scaling of an existing distributed vector with the
   * coefficient of a constant
   */
  def multiply(alpha: Double): DistributedVector = {
    new DistributedVector(multiplyScalar(alpha), _size)
  }
  private def multiplyScalar(alpha: Double): RDD[dvEntry] = {
    entries.map(x => dvEntry(x.idx, x.v * alpha))
  }

  def *(alpha: Double): DistributedVector = {
    multiply(alpha)
  }

  /*
   * divide(alpha: Double) computes element-wise division for a distributed vector
   */
  def divide(alpha: Double): DistributedVector = {
    require(alpha != 0.0, s"division requires a non-zero divident $alpha")
    multiply(1.0 / alpha)
  }

  def /(alpha: Double): DistributedVector = {
    divide(alpha)
  }

  /*
   * add(other: DistriubtedVector) computes the addition of two distributed vectors
   */
  def add(other: DistributedVector): DistributedVector = {
    val s1 = size
    val s2 = other.size
    require(s1 == s2, s"Two distributed vectors must have the same dimension to add together, s1 = $s1, s2 = $s2")
    val selfvec = entries.map(x => (x.idx, x.v))
    val othervec = other.entries.map(x => (x.idx, x.v))
    //selfvector.foreach(item => println("[" + item._1 + ", " + item._2 + "]"))
    //otherVector.foreach(item => println("[" + item._1 + ", " + item._2 + "]"))
    val addEntries = selfvec.cogroup(othervec).map {entry =>
      val key = entry._1
      var value = 0.0
      value += entry._2._1.sum
      value += entry._2._2.sum
      (key, value)
    }
    //addEntries.foreach(item => println("[" + item._1 + ", " + item._2 + "]"))
    //val numPartition = entries.partitions.length  // #partitions for the original distributed vector

    val convert2vector = (iter: Iterator[(Long, Double)]) => {
      iter.map(x => dvEntry(x._1, x._2))
      /*var elems = iter.toList
      elems = elems.sorted
      val start = elems.min._1
      //println("start = " + start)
      val len = elems.length
      //println("iter.len = " + len)
      var res = List[dvEntry]()
      for (i <- 0 until len) {
          res = res :+ dvEntry(elems(i)._1, elems(i)._2)
      }
      res.iterator */
    }
    //val entriesRepartition = addEntries.partitionBy(new RangePartitioner(numPartition, addEntries))

    //val addEntriesRDD = entriesRepartition.mapPartitions(convert2vector, true)
    val addEntriesRDD = addEntries.mapPartitions(convert2vector, true)
    new DistributedVector(addEntriesRDD, s1)
  }

  def +(other: DistributedVector): DistributedVector = {
    add(other)
  }

  /*
   * print the distributed vector partition by partition, this function should be used for debugging purpose only
   */
  def display() = {
    entries.foreachPartition{iter =>
        while (iter.hasNext) {
            print(iter.next() + ", ")
        }
    }
  }

}

/*
 * Companion object for DistributedVector to provide a factory method for
 * creating certain kind of distributed vectors.
 */
object DistributedVector {
    def OnesVector(idxes: RDD[Long]): DistributedVector = {
        val vecRdd = idxes.map(x => dvEntry(x, 1.0))
        new DistributedVector(vecRdd, 0)
    }


    /*def allOnesVector(sc: SparkContext, size: Long, capacity: Int = 16e6.toInt): DistributedVector = {
        // How large should each offsetVector be?
        // Each Double takes 8B and suppose each partition takes size of 128MB.
        // This means each partition holds 128MB / 8B = 16M elements.
        //val capacity = 16e6.toInt
        val npartition = size / capacity
        val last = size - (npartition - 1) * capacity
        if (npartition < 1) {
            val values = List.fill(size.toInt)(1.0).toArray
            val content = List(new offsetVector(0, Vectors.dense(values)))
            new DistributedVector(sc.makeRDD(content), size)
        }
      else {
            val content = List[offsetVector]()
            for (i <- 0 until (npartition - 1).toInt) {
                val values = List.fill(capacity)(1.0).toArray
                content.::(new offsetVector(i * capacity, Vectors.dense(values)))
            }
            val values = List.fill(last.toInt)(1.0).toArray
            content.::(new offsetVector((npartition - 1) * capacity, Vectors.dense(values)))
            println("finishing filling for ones!")
            new DistributedVector(sc.parallelize(content), size)
        }
    }*/
}


object TestDistributedVector {
    def main (args: Array[String]) {
      val conf = new SparkConf()
                  .setMaster("local[4]")
                  .setAppName("Test for distributed vectors")
                  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                  .set("spark.shuffle.consolidateFiles", "true")
                  .set("spark.shuffle.compress", "false")
      //conf.registerKryoClasses(Array(classOf[offsetVector], classOf[DistributedVector]))
      val sc = new SparkContext(conf)
      val vec1 = List(dvEntry(0L,1.0), dvEntry(1L,1.0), dvEntry(2L, 1.0), dvEntry(3L,1.0), dvEntry(11L,2.0), dvEntry(15L,2.0), dvEntry(16L,2.0), dvEntry(19L,2.0))
      val vec2 = List(dvEntry(1L,2.0), dvEntry(3L,2.0), dvEntry(6L,2.0), dvEntry(9L,2.0), dvEntry(10L,1.0), dvEntry(11L,1.0), dvEntry(12L, 1.0), dvEntry(13L,1.0))
      val data = sc.parallelize(vec1, 2)
      val data2 = sc.parallelize(vec2, 2)
      val distVector1 = new DistributedVector(data, 20)
      val distVector2 = new DistributedVector(data2, 20)
      val vec = Vectors.dense(Array[Double](1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1))
      //println(distVector1.norm(Double.PositiveInfinity))
      //println(distVector1.inner(distVector2))
      //val distVector5 = distVector1.multiply(5.0)
      //distVector5.print()
      //distVector1.add(distVector2).display()
      println(distVector1.inner(vec))
      val list = (0L until 1000L).toList
      val rdd = sc.parallelize(list, 3)
      var y = DistributedVector.OnesVector(rdd)
      println(y.size)
      sc.stop()
  }
}
