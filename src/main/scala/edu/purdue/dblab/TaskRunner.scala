package edu.purdue.dblab

import java.util.concurrent.{LinkedBlockingQueue, ConcurrentLinkedQueue}

import org.apache.spark.{TaskContext, Logging}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
 * Created by yongyangyu on 9/11/15.
 */

class BlockingIterator[T](val queue: LinkedBlockingQueue[T],
                          val numResult: Int) extends Iterator[T] with Logging {
    var result = 0
    def hasNext: Boolean = result < numResult

    def next(): T = {
        result += 1
        return queue.take();
    }
}

class MultiplyInPlaceTaskRunner (concurrentLevel: Int) extends Serializable with Logging {
    def run (iter: Iterator[((Int, Int), (MLMatrix, MLMatrix))],
             context: TaskContext): Iterator[((Int, Int), MLMatrix)] = {
        def update(buf: ArrayBuffer[(MLMatrix, MLMatrix)]): MLMatrix = {
            var blk: MLMatrix = null.asInstanceOf[MLMatrix]
            for (t <- buf) {
                blk match {
                  case null =>
                    blk = DenseMatrix.zeros(t._1.numRows, t._2.numCols)
                    blk = LocalMatrix.incrementalMultiply(t._1, t._2, blk)
                  case block: MLMatrix => blk = LocalMatrix.incrementalMultiply(t._1, t._2, blk)
                }
            }
          blk
        }

        val combiners = new HashMap[(Int, Int), ArrayBuffer[(MLMatrix, MLMatrix)]]()
        iter.foreach{ case (idx, pair) =>
            val tmp = combiners.getOrElse(idx, new ArrayBuffer[(MLMatrix, MLMatrix)]())
            tmp.append(pair)
            combiners.put(idx, tmp)
        }

        val taskQ = new ConcurrentLinkedQueue[((Int, Int), ArrayBuffer[(MLMatrix, MLMatrix)])]()
        var num = 0
        combiners.iterator.foreach{ case (idx, buf) =>
            num += 1
            taskQ.add((idx, buf))
        }

        val resultQ = new LinkedBlockingQueue[((Int, Int), MLMatrix)](2)
        for (i <- 0 until concurrentLevel) {
            new Thread() {
                override def run(): Unit = {
                    var isEmpty = false
                    while (!isEmpty) {
                        taskQ.poll() match {
                          case null => isEmpty = true
                          case (idx: (Int, Int), buf: ArrayBuffer[(MLMatrix, MLMatrix)]) =>
                            resultQ.put((idx, update(buf)))
                        }
                    }
                }
            }.start()
        }
        new BlockingIterator[((Int, Int), MLMatrix)](resultQ, num)
    }
}

class ConcurrentTaskRunner(
      concurrentLevel: Int,
      createCombiner: ((MLMatrix, MLMatrix)) => MLMatrix,
      mergeCombiner: (MLMatrix, MLMatrix) => MLMatrix) extends Serializable with Logging {

    def update(buf: ArrayBuffer[(MLMatrix, MLMatrix)], context: TaskContext): MLMatrix = {
        var res: MLMatrix = null.asInstanceOf[MLMatrix]
        for (v <- buf) {
            res match {
              case c: MLMatrix =>
                  val t = createCombiner(v)
                  res = mergeCombiner(c, t)
              case null => createCombiner(v)
            }
        }
      res
    }

    def run(iter: Iterator[((Int, Int), (MLMatrix, MLMatrix))], context: TaskContext): Iterator[((Int, Int), MLMatrix)] = {
        val combiners = new mutable.HashMap[(Int, Int), ArrayBuffer[(MLMatrix, MLMatrix)]]()
        iter.foreach { case (k, v) =>
            combiners.getOrElseUpdate(k, ArrayBuffer()) += v
        }

        val concurrentLinkedQueue = new ConcurrentLinkedQueue[((Int, Int), ArrayBuffer[(MLMatrix, MLMatrix)])]()
        var num = 0
        combiners.iterator.foreach { case (k, buf) =>
            concurrentLinkedQueue.add((k, buf))
            num += 1
        }

        val result = new LinkedBlockingQueue[((Int, Int), MLMatrix)](2)
        for (i <- 0 until concurrentLevel) {
            new Thread() {
                override def run(): Unit = {
                    var flag = true
                    while (flag) {
                        val entry = concurrentLinkedQueue.poll()
                        entry match {
                          case null => flag = false
                          case (index: (Int, Int), buf: ArrayBuffer[(MLMatrix, MLMatrix)]) =>
                              result.put((index, update(buf, context)))
                        }
                    }
                }
            }.start()
        }
        new BlockingIterator[((Int, Int), MLMatrix)](result, num)
    }
}
