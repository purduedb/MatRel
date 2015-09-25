package edu.purdue.dblab

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.concurrent.TrieMap
//import java.util.concurrent.ConcurrentHashMap
//import scala.collection.JavaConversions.asScalaIterator

import scala.collection.mutable
import scala.collection.mutable.{Map => MMap, ArrayBuffer}
import breeze.linalg.{Matrix => BM}

/**
 * Created by yongyangyu on 7/15/15.
 */
class BlockPartitionMatrix (
    var blocks: RDD[((Int, Int), MLMatrix)],
    val ROWS_PER_BLK: Int,
    val COLS_PER_BLK: Int,
    private var nrows: Long,
    private var ncols: Long) extends Matrix with Logging {

    val ROW_BLK_NUM = math.ceil(nRows() * 1.0 / ROWS_PER_BLK).toInt
    val COL_BLK_NUM = math.ceil(nCols() * 1.0 / COLS_PER_BLK).toInt

    private var groupByCached: RDD[(Int, Iterable[(Int, MLMatrix)])] = null

    private val numPartitions: Int = 8 // 8 workers

    private var sparsity: Double = 0.0


    override def nRows(): Long = {
        if (nrows <= 0L) {
            getDimension()
        }
        nrows
    }

    override def nCols(): Long = {
        if (ncols <= 0L) {
            getDimension()
        }
        ncols
    }

    override def nnz(): Long = {
        blocks.map{ mat =>
            mat._2 match {
              case mdense: DenseMatrix => mdense.values.count( _ != 0).toLong
              case msparse: SparseMatrix => msparse.values.count( _ != 0).toLong
              case _ => 0L
            }
        }.sum().toLong
    }

    // sparsity is defined as nnz / (m * n) where m and n are number of rows and cols
    // Is this a fast operation? Does there exist any better way to get the precise
    // sparsity info of the underlying distributed matrix.
    def getSparsity(): Double = {
        if (sparsity <= 0) {
            val nnz = blocks.map { case ((rid, cid), mat) =>
                mat match {
                    case den: DenseMatrix => den.values.length
                    case sp: SparseMatrix => sp.values.length
                }
            }.reduce(_ + _)
            sparsity = nnz * 1.0 / (nRows() * nCols())
        }
        sparsity
    }

    def stat() = {
        println("-" * 40 )
        println(s"Block partition matrix has $ROW_BLK_NUM row blks")
        println(s"Block partition matrix has $COL_BLK_NUM col blks")
        println(s"Each block size is $ROWS_PER_BLK by $COLS_PER_BLK")
        println(s"Matrix has ${nRows()} rows")
        println(s"Matrix has ${nCols()} cols")
        println("-" * 40 )
    }

    def partitioner = blocks.partitioner.get

    private type MatrixBlk = ((Int, Int), MLMatrix)
    private type PartitionScheme = (Int, Int)

    private def genBlockPartitioner(): BlockCyclicPartitioner = {
        val scale = 1.0 / math.sqrt(numPartitions)
        //println(s"In genBlockPartitioner: ROW_BLKS = $ROW_BLK_NUM")
        //println(s"In genBlockPartitioner: COL_BLKS = $COL_BLK_NUM")
        val ROW_BLKS_PER_PARTITION = math.round(math.max(scale * ROW_BLK_NUM, 1.0)).toInt
        val COL_BLKS_PER_PARTITION = math.round(math.max(scale * COL_BLK_NUM, 1.0)).toInt
        new BlockCyclicPartitioner(ROW_BLK_NUM, COL_BLK_NUM, ROW_BLKS_PER_PARTITION, COL_BLKS_PER_PARTITION)
    }

    private lazy val blkInfo = blocks.mapValues(block => (block.numRows, block.numCols)).cache()

    private def getDimension(): Unit = {
        val (rows, cols) = blkInfo.map { x =>
            val blkRowIdx = x._1._1
            val blkColIdx = x._1._1
            val rowSize = x._2._1
            val colSize = x._2._2
            (blkRowIdx.toLong * ROWS_PER_BLK + rowSize, blkRowIdx.toLong * COLS_PER_BLK + colSize)
        }.reduce { (x, y) =>
          (math.max(x._1, y._1), math.max(x._2, y._2))
        }
        if (nrows <= 0) nrows = rows
        assert(rows <= nrows, s"Number of rows $rows is more than claimed $nrows")
        if (ncols <= 0) ncols = cols
        assert(cols <= ncols, s"Number of cols $cols is more than claimed $ncols")
    }

    /*
     * Caches the underlying RDD
     */
    def cache(): this.type = {
        blocks.cache()
        this
    }

    def partitionBy(p: Partitioner): this.type = {
        blocks = blocks.partitionBy(p)
        this
    }

    def partitionByBlockCyclic(): this.type = {
        blocks = blocks.partitionBy(genBlockPartitioner())
        this
    }

    /*
     * Validates the block matrix to find out any errors or exceptions.
     */
    def validate(): Unit = {
        logDebug("Validating block partition matrices ...")
        getDimension()
        logDebug("Block partition matrices dimensions are OK ...")
        // check if there exists duplicates for the keys, i.e., duplicate indices
        blkInfo.countByKey().foreach{ case (key, count) =>
            if (count > 1) {
                throw new SparkException(s"Found duplicate indices for the same block, key is $key.")
            }
        }
        logDebug("Block partition matrices indices are OK ...")
        val dimMsg = s"Dimensions different than ROWS_PER_BLK: $ROWS_PER_BLK, and " +
                    s"COLS_PER_BLK: $COLS_PER_BLK. Blocks on the right and bottom edges may have " +
                    s"smaller dimensions. The problem may be fixed by repartitioning the matrix."
        // check for size of each individual block
        blkInfo.foreach{ case ((blkRowIdx, blkColInx), (m, n)) =>
            if ((blkRowIdx < ROW_BLK_NUM - 1 && m != ROWS_PER_BLK) ||
                (blkRowIdx == ROW_BLK_NUM - 1 && (m <= 0 || m > ROWS_PER_BLK))) {
                throw new SparkException(s"Matrix block at ($blkRowIdx, $blkColInx) has " + dimMsg)
            }
            if ((blkColInx < COL_BLK_NUM - 1 && n != COLS_PER_BLK) ||
                (blkColInx == COL_BLK_NUM - 1 && (n <= 0 || n > COLS_PER_BLK))) {
                throw new SparkException(s"Matrix block at ($blkRowIdx, $blkColInx) has " + dimMsg)
            }
        }
        logDebug("Block partition matrix dimensions are OK ...")
        logDebug("Block partition matrix is valid.")
    }

    def transpose(): BlockPartitionMatrix = {
        val transposeBlks = blocks.map {
            case ((blkRowIdx, blkColIdx), mat) => ((blkColIdx, blkRowIdx), mat.transpose)
        }
        new BlockPartitionMatrix(transposeBlks, COLS_PER_BLK, ROWS_PER_BLK, nCols(), nRows())
    }

    def t: BlockPartitionMatrix = transpose()

    /*
     * Collect the block partitioned matrix on the driver side for debugging purpose only.
     */
    def toLocalMatrix(): MLMatrix = {
        require(nRows() < Int.MaxValue, s"Number of rows should be smaller than Int.MaxValue, but " +
        s"found ${nRows()}")
        require(nCols() < Int.MaxValue, s"Number of cols should be smaller than Int.MaxValue, but " +
        s"found ${nCols()}")
        require(nnz() < Int.MaxValue, s"Total number of the entries should be smaller than Int.MaxValue, but " +
        s"found ${nnz()}")
        val m = nRows().toInt
        val n = nCols().toInt
        val memSize = m * n / 131072  // m-by-n * 8 byte / (1024 * 1024) MB
        if (memSize > 500) logWarning(s"Storing local matrix requires $memSize MB")
        val localBlks = blocks.collect()
        val values = Array.fill(m * n)(0.0)
        localBlks.foreach{
            case ((blkRowIdx, blkColIdx), mat) =>
                val rowOffset = blkRowIdx * ROWS_PER_BLK
                val colOffset = blkColIdx * COLS_PER_BLK
                // (i, j) --> (i + rowOffset, j + colOffset)
                for (i <- 0 until mat.numRows; j <- 0 until mat.numCols) {
                    val indexOffset = (j + colOffset) * m + (rowOffset + i)
                    values(indexOffset) = mat(i, j)
                }
        }
        new DenseMatrix(m, n, values)
    }

    def *(alpha: Double): BlockPartitionMatrix = {
        multiplyScalar(alpha)
    }

    def *(other: BlockPartitionMatrix, partitioner: Partitioner): BlockPartitionMatrix = {
        require(nRows() == other.nRows(), s"Two matrices must have the same number of rows. " +
          s"A.rows: ${nRows()}, B.rows: ${other.nRows()}")
        require(nCols() == other.nCols(), s"Two matrices must have the same number of cols. " +
          s"A.cols: ${nCols()}, B.cols: ${other.nCols()}")
        var rdd1 = blocks
        if (!rdd1.partitioner.get.isInstanceOf[partitioner.type]) {
            rdd1 = rdd1.partitionBy(partitioner)
        }
        var rdd2 = other.blocks
        if (!rdd2.partitioner.get.isInstanceOf[partitioner.type]) {
            rdd2 = rdd2.partitionBy(partitioner)
        }
        val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) {
            case (iter1, iter2) =>
                val idx2val = new TrieMap[(Int, Int), MLMatrix]()
                val res = new TrieMap[(Int, Int), MLMatrix]()
                for (elem <- iter1) {
                    val key = elem._1
                    if (!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2)
                }
                for (elem <- iter2) {
                    val key = elem._1
                    if (idx2val.contains(key)) {
                        val tmp = idx2val.get(key).get
                        res.putIfAbsent(key, LocalMatrix.elementWiseMultiply(tmp, elem._2))
                    }
                }
            res.iterator
        }
        new BlockPartitionMatrix(rdd, ROWS_PER_BLK, COLS_PER_BLK, nRows(), nCols())
    }

    def *:(alpha: Double): BlockPartitionMatrix = {
        multiplyScalar(alpha)
    }

    def /(alpha: Double): BlockPartitionMatrix = {
        require(alpha != 0, "Block matrix divided by 0 error!")
        multiplyScalar(1.0 / alpha)
    }

    def /:(alpha: Double): BlockPartitionMatrix = {
        require(alpha != 0, "Block matrix divided by 0 error!")
        multiplyScalar(1.0 / alpha)
    }

    def /(other: BlockPartitionMatrix, partitioner: Partitioner): BlockPartitionMatrix = {
        require(nRows() == other.nRows(), s"Two matrices must have the same number of rows. " +
          s"A.rows: ${nRows()}, B.rows: ${other.nRows()}")
        require(nCols() == other.nCols(), s"Two matrices must have the same number of cols. " +
          s"A.cols: ${nCols()}, B.cols: ${other.nCols()}")
        var rdd1 = blocks
        if (!rdd1.partitioner.get.isInstanceOf[partitioner.type]) {
            rdd1 = rdd1.partitionBy(partitioner)
        }
        var rdd2 = other.blocks
        if (!rdd2.partitioner.get.isInstanceOf[partitioner.type]) {
            rdd2 = rdd2.partitionBy(partitioner)
        }
        val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) {
            case (iter1, iter2) =>
                val idx2val = new TrieMap[(Int, Int), MLMatrix]()
                val res = new TrieMap[(Int, Int), MLMatrix]()
                for (elem <- iter1) {
                    val key = elem._1
                    if (!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2)
                }
                for (elem <- iter2) {
                    val key = elem._1
                    if (idx2val.contains(key)) {
                        val tmp = idx2val.get(key).get
                        res.putIfAbsent(key, LocalMatrix.elementWiseDivide(tmp, elem._2))
                    }
                }
                res.iterator
        }
        new BlockPartitionMatrix(rdd, ROWS_PER_BLK, COLS_PER_BLK, nRows(), nCols())
    }

    def multiplyScalar(alpha: Double): BlockPartitionMatrix = {
        /*println(blocks.partitions.length + " partitions in blocks RDD" +
          s" with ${nRows()} rows ${nCols()} cols")
        blocks.mapPartitionsWithIndex{ case (id, iter) =>
            var count = 0
            for (tuple <- iter) {
                tuple._2 match {
                    case dm: DenseMatrix => count += dm.values.length
                    case sp: SparseMatrix => count += sp.values.length
                    case _ => throw new SparkException("Format wrong")
                }
            }
            Iterator((id, count))
        }.collect().foreach(println)*/

        val rdd = blocks.mapValues(mat => LocalMatrix.multiplyScalar(alpha, mat))
        new BlockPartitionMatrix(rdd, ROWS_PER_BLK, COLS_PER_BLK, nRows(), nCols())
    }

    def multiplyScalarInPlace(alpha: Double): BlockPartitionMatrix = {
        blocks.foreach { case ((rowIdx, colIdx), mat) =>
            mat.update(x => alpha * x)
        }
        this
    }

    def +(other: BlockPartitionMatrix,
          dimension: PartitionScheme = (ROWS_PER_BLK, COLS_PER_BLK),
          partitioner: Partitioner): BlockPartitionMatrix = {
        add(other, dimension, partitioner)
    }

    /*
     * Adds two block partitioned matrices together. The matrices must have the same size but may have
     * different partitioning schemes, i.e., different `ROWS_PER_BLK` and `COLS_PER_BLK` values.
     * @param dimension, specifies the (ROWS_PER_PARTITION, COLS_PER_PARTITION) of the result
     */
    def add(other: BlockPartitionMatrix,
            dimension: PartitionScheme = (ROWS_PER_BLK, COLS_PER_BLK),
            partitioner: Partitioner): BlockPartitionMatrix = {
        val t1 = System.currentTimeMillis()
        require(nRows() == other.nRows(), s"Two matrices must have the same number of rows. " +
        s"A.rows: ${nRows()}, B.rows: ${other.nRows()}")
        require(nCols() == other.nCols(), s"Two matrices must have the same number of cols. " +
        s"A.cols: ${nCols()}, B.cols: ${other.nCols()}")
        // simply case when both matrices are partitioned in the same fashion
        if (ROWS_PER_BLK == other.ROWS_PER_BLK && COLS_PER_BLK == other.COLS_PER_BLK &&
            ROWS_PER_BLK == dimension._1 && COLS_PER_BLK == dimension._2) {
            addSameDim(blocks, other.blocks, ROWS_PER_BLK, COLS_PER_BLK)
        }
        // need to repartition the matrices according to the specification
        else {
            var (repartA, repartB) = (true, true)
            if (ROWS_PER_BLK == dimension._1 && COLS_PER_BLK == dimension._2) {
                repartA = false
            }
            if (other.ROWS_PER_BLK == dimension._1 && other.COLS_PER_BLK == dimension._2) {
                repartB = false
            }
            var (rddA, rddB) = (blocks, other.blocks)
            if (repartA) {
                rddA = getNewBlocks(blocks, ROWS_PER_BLK, COLS_PER_BLK,
                    dimension._1, dimension._2, partitioner)
                /*rddA.foreach{
                    x =>
                        val (row, col) = x._1
                        val mat = x._2
                        println(s"row: $row, col: $col, Matrix A")
                        println(mat)
                }*/
            }
            if (repartB) {
                rddB = getNewBlocks(other.blocks, other.ROWS_PER_BLK, other.COLS_PER_BLK,
                    dimension._1, dimension._2, partitioner)
                /*rddB.foreach{
                    x =>
                        val (row, col) = x._1
                        val mat = x._2
                        println(s"row: $row, col: $col, Matrix B")
                        println(mat)
                }*/
            }
            // place holder
            val t2 = System.currentTimeMillis()
            println("Matrix addition takes: " + (t2-t1)/1000.0 + " sec")
            addSameDim(rddA, rddB, dimension._1, dimension._2)
        }
    }

    // rddA and rddB already partitioned in the same way
    private def addSameDim(rddA: RDD[MatrixBlk], rddB: RDD[MatrixBlk],
                            RPB: Int, CPB: Int): BlockPartitionMatrix = {
        val rdd = rddA.zipPartitions(rddB, preservesPartitioning = true) { (iter1, iter2) =>
            val buf = new TrieMap[(Int, Int), MLMatrix]()
            for (a <- iter1) {
                if (a != null) {
                    val idx = a._1
                    if (!buf.contains(idx)) buf.putIfAbsent(idx, a._2)
                    else {
                        val old = buf.get(idx).get
                        buf.put(idx, LocalMatrix.add(old, a._2))
                    }
                }
            }
            for (b <- iter2) {
                if (b != null) {
                    val idx = b._1
                    if (!buf.contains(idx)) buf.putIfAbsent(idx, b._2)
                    else {
                        val old = buf.get(idx).get
                        buf.put(idx, LocalMatrix.add(old, b._2))
                    }
                }
            }
            buf.iterator
        }
        new BlockPartitionMatrix(rdd, RPB, CPB, nRows(), nCols())
        /*val addBlks = rddA.cogroup(rddB, genBlockPartitioner())
          .map {
            case ((rowIdx, colIdx), (a, b)) =>
                if (a.size > 1 || b.size > 1) {
                    throw new SparkException("There are multiple MatrixBlocks with indices: " +
                      s"($rowIdx, $colIdx). Please remove the duplicate and try again.")
                }
                if (a.isEmpty) {
                    new MatrixBlk((rowIdx, colIdx), b.head)
                }
                else if (b.isEmpty) {
                    new MatrixBlk((rowIdx, colIdx), a.head)
                }
                else {
                    new MatrixBlk((rowIdx, colIdx), LocalMatrix.add(a.head, b.head))
                }
        }
        new BlockPartitionMatrix(addBlks, RPB, CPB, nRows(), nCols()) */
    }

    private def getNewBlocks(rdd: RDD[MatrixBlk],
                             curRPB: Int, curCPB: Int,
                             targetRPB: Int, targetCPB: Int,
                             partitioner: Partitioner): RDD[MatrixBlk] = {
        val rddNew = rePartition(rdd, curRPB, curCPB, targetRPB, targetCPB)
        rddNew.groupByKey(genBlockPartitioner()).map {
            case ((rowIdx, colIdx), iter) =>
                val rowStart = rowIdx * targetRPB
                val rowEnd = math.min((rowIdx + 1) * targetRPB - 1, nRows() - 1)
                val colStart = colIdx * targetCPB
                val colEnd = math.min((colIdx + 1) * targetCPB - 1, nCols() - 1)
                val (m, n) = (rowEnd.toInt - rowStart + 1, colEnd.toInt - colStart + 1)  // current blk size
                val values = Array.fill(m * n)(0.0)
                val (rowOffset, colOffset) = (rowIdx * targetRPB, colIdx * targetCPB)
                for (elem <- iter) {
                    var arr = elem._5
                    var rowSize = elem._2 - elem._1 + 1
                    for (j <- elem._3 to elem._4; i <- elem._1 to elem._2) {
                        var idx = (j - elem._3) * rowSize + (i - elem._1)
                        // assign arr(idx) to a proper position
                        var (ridx, cidx) = (i - rowOffset, j - colOffset)
                        values(cidx.toInt * m + ridx.toInt) = arr(idx.toInt)
                    }
                }
                // 10% or more 0 elements, use sparse matrix format (according to EDBT'15 paper)
                if (values.count(entry => entry > 0.0) > 0.1 * values.length ) {
                    ((rowIdx, colIdx), new DenseMatrix(m, n, values))
                }
                else {
                    ((rowIdx, colIdx), new DenseMatrix(m, n, values).toSparse)
                }

        }.partitionBy(partitioner)

    }
    // RPB -- #rows_per_blk, CPB -- #cols_per_blk
    private def rePartition(rdd: RDD[MatrixBlk],
                            curRPB: Int, curCPB: Int, targetRPB: Int,
                            targetCPB: Int): RDD[((Int, Int), (Long, Long, Long, Long, Array[Double]))] = {
        rdd.map { case ((rowIdx, colIdx), mat) =>
            val rowStart: Long = rowIdx * curRPB
            val rowEnd: Long = math.min((rowIdx + 1) * curRPB - 1, nRows() - 1)
            val colStart: Long = colIdx * curCPB
            val colEnd: Long = math.min((colIdx + 1) * curCPB - 1, nCols() - 1)
            val (x1, x2) = ((rowStart / targetRPB).toInt, (rowEnd / targetRPB).toInt)
            val (y1, y2) = ((colStart / targetCPB).toInt, (colEnd / targetCPB).toInt)
            val res = ArrayBuffer[((Int, Int), (Long, Long, Long, Long, Array[Double]))]()
            for (r <- x1 to x2; c <- y1 to y2) {
                // (r, c) entry for the target partition scheme
                val rowStartNew: Long = r * targetRPB
                val rowEndNew: Long = math.min((r + 1) * targetRPB - 1, nRows() - 1)
                val colStartNew: Long = c * targetCPB
                val colEndNew: Long = math.min((c + 1) * targetCPB - 1, nCols() - 1)
                val rowRange = findIntersect(rowStart, rowEnd, rowStartNew, rowEndNew)
                val colRange = findIntersect(colStart, colEnd, colStartNew, colEndNew)
                val (rowOffset, colOffset) = (rowIdx * curRPB, colIdx * curCPB)
                val values = ArrayBuffer[Double]()
                for (j <- colRange; i <- rowRange) {
                    values += mat((i - rowOffset).toInt, (j - colOffset).toInt)
                }
                val elem = (rowRange(0), rowRange(rowRange.length - 1),
                  colRange(0), colRange(colRange.length - 1), values.toArray)
                val entry = ((r, c), elem)
                res += entry
            }
            res.toArray
        }.flatMap(x => x)
    }

    private def findIntersect(s1: Long, e1: Long, s2: Long, e2: Long): Array[Long] = {
        val tmp = ArrayBuffer[Long]()
        var (x, y) = (s1, s2)
        while (x <= e1 && y <= e2) {
            if (x == y) {
                tmp += x
                x += 1
                y += 1
            }
            else if (x < y) {
                x += 1
            }
            else {
                y += 1
            }
        }
        tmp.toArray
    }

    def %*%(other: BlockPartitionMatrix): BlockPartitionMatrix = {
        multiply(other)
        //blockMultiplyDup(other)
    }

    // TODO: currently the repartitioning of A * B will perform on matrix B
    // TODO: if blocks of B do not conform with blocks of A, need to find an optimal strategy
    def multiply(other: BlockPartitionMatrix): BlockPartitionMatrix = {
        val t1 = System.currentTimeMillis()
        require(nCols() == other.nRows(), s"#cols of A should be equal to #rows of B, but found " +
        s"A.numCols = ${nCols()}, B.numRows = ${other.nRows()}")
        var rddB = other.blocks
        //var useOtherMatrix: Boolean = false
        if (COLS_PER_BLK != other.ROWS_PER_BLK) {
            logWarning(s"Repartition Matrix B since A.col_per_blk = $COLS_PER_BLK " +
              s"and B.row_per_blk = ${other.ROWS_PER_BLK}")
            rddB = getNewBlocks(other.blocks, other.ROWS_PER_BLK, other.COLS_PER_BLK,
                COLS_PER_BLK, COLS_PER_BLK, new RowPartitioner(numPartitions))
            //useOtherMatrix = true
        }
        // other.ROWS_PER_BLK = COLS_PER_BLK and square blk for other
        val OTHER_COL_BLK_NUM = math.ceil(other.nCols() * 1.0 / COLS_PER_BLK).toInt
        //val otherMatrix = new BlockPartitionMatrix(rddB, COLS_PER_BLK, COLS_PER_BLK, other.nRows(), other.nCols())
        //val resPartitioner = BlockCyclicPartitioner(ROW_BLK_NUM, OTHER_COL_BLK_NUM, numWorkers)

        val resPartitioner = new RowPartitioner(numPartitions)
        if (groupByCached == null) {
            groupByCached = blocks.map{ case ((rowIdx, colIdx), matA) =>
                (colIdx, (rowIdx, matA))
            }.groupByKey().cache()
        }
        val rdd1 = groupByCached
        val rdd2 = rddB.map{ case ((rowIdx, colIdx), matB) =>
            (rowIdx, (colIdx, matB))
        }.groupByKey()
        val rddC = rdd1.join(rdd2)
          .values
          .flatMap{ case (iterA, iterB) =>
            val product = mutable.ArrayBuffer[((Int, Int), BM[Double])]()
            for (blk1 <- iterA) {
                for (blk2 <- iterB) {
                    val idx = (blk1._1, blk2._1)
                    val c = LocalMatrix.matrixMultiplication(blk1._2, blk2._2)
                    product += ((idx, LocalMatrix.toBreeze(c)))
                }
            }
            product
        }.combineByKey(
            (x: BM[Double]) => x,
            (acc: BM[Double], x) => acc + x,
            (acc1: BM[Double], acc2: BM[Double]) => acc1 + acc2,
            resPartitioner, true, null
        ).mapValues(LocalMatrix.fromBreeze(_))
        val t2 = System.currentTimeMillis()
        println("Matrix multiplication takes: " + (t2-t1)/1000.0 + " sec")
        new BlockPartitionMatrix(rddC, ROWS_PER_BLK, COLS_PER_BLK, nRows(), other.nCols())
    }

    // just for comparison purpose, not used in the multiplication code
    def multiplyDMAC(other: BlockPartitionMatrix): BlockPartitionMatrix = {
        require(nCols() == other.nRows(), s"#cols of A should be equal to #rows of B, but found " +
          s"A.numCols = ${nCols()}, B.numRows = ${other.nRows()}")
        var rddB = other.blocks
        //var useOtherMatrix: Boolean = false
        if (COLS_PER_BLK != other.ROWS_PER_BLK) {
            logWarning(s"Repartition Matrix B since A.col_per_blk = $COLS_PER_BLK and B.row_per_blk = ${other.ROWS_PER_BLK}")
            rddB = getNewBlocks(other.blocks, other.ROWS_PER_BLK, other.COLS_PER_BLK,
                COLS_PER_BLK, COLS_PER_BLK, new RowPartitioner(numPartitions))
            //useOtherMatrix = true
        }
        // other.ROWS_PER_BLK = COLS_PER_BLK and square blk for other
        val OTHER_COL_BLK_NUM = math.ceil(other.nCols() * 1.0 / COLS_PER_BLK).toInt
        //val otherMatrix = new BlockPartitionMatrix(rddB, COLS_PER_BLK, COLS_PER_BLK, other.nRows(), other.nCols())
        //val resPartitioner = BlockCyclicPartitioner(ROW_BLK_NUM, OTHER_COL_BLK_NUM, math.max(blocks.partitions.length, rddB.partitions.length))

        val nodes = 8

        val aggregator = new Aggregator[(Int, Int), (MLMatrix, MLMatrix),
          ArrayBuffer[(MLMatrix, MLMatrix)]](createCombiner, mergeValue, mergeCombiner)

        val rdd1 = blocks.map{ case ((rowIdx, colIdx), matA) =>
            (colIdx % nodes, (colIdx, rowIdx, matA))
        }.groupByKey()
        val rdd2 = rddB.map{ case ((rowIdx, colIdx), matB) =>
            (rowIdx % nodes, (rowIdx, colIdx, matB))
        }.groupByKey()

        val rddC = rdd1.join(rdd2)
          .values.flatMap{ case (buf1, buf2) =>
            val cross = new ArrayBuffer[((Int, Int), (MLMatrix, MLMatrix))]()
            for (i <- buf1)
                for (j <- buf2) {
                    if (i._1 == j._1)
                        cross.append(((i._2, j._2), (i._3, j._3)))
                }
            cross
        }.mapPartitionsWithContext((context, iter) => {
            new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
        }, true).map { case (index, buf) =>
            var re_block: MLMatrix = null
            for ((blk1, blk2) <- buf) {
                val mul = LocalMatrix.matrixMultiplication(blk1, blk2)
                re_block match {
                    case null => re_block = mul
                    case _ => re_block = LocalMatrix.add(mul, re_block)
                }
            }
            (index, re_block)
        }.reduceByKey(LocalMatrix.add(_, _))
        new BlockPartitionMatrix(rddC, ROWS_PER_BLK, COLS_PER_BLK, nRows(), other.nCols())
    }

    def createCombiner (v: (MLMatrix, MLMatrix)) = ArrayBuffer(v)

    def mergeValue(cur: ArrayBuffer[(MLMatrix, MLMatrix)], v: (MLMatrix, MLMatrix)) = cur += v

    def mergeCombiner(c1 : ArrayBuffer[(MLMatrix, MLMatrix)], c2 : ArrayBuffer[(MLMatrix, MLMatrix)]) = c1 ++ c2


    /*
     * Compute top-k largest elements from the block partitioned matrix.
     */
    def topK(k: Int): Array[((Long, Long), Double)] = {
        val res = blocks.map { case ((rowIdx, colIdx), matrix) =>
            val pq = new mutable.PriorityQueue[((Int, Int), Double)]()(Ordering.by(orderByValueInt))
            for (i <- 0 until matrix.numRows; j <- 0 until matrix.numCols) {
                if (pq.size < k) {
                    pq.enqueue(((i, j), matrix(i, j)))
                }
                else {
                    pq.enqueue(((i, j), matrix(i, j)))
                    pq.dequeue()
                }
            }
            ((rowIdx, colIdx), pq.toArray)
        }.collect()
        val q = new mutable.PriorityQueue[((Long, Long), Double)]()(Ordering.by(orderByValueDouble))
        for (((blk_row, blk_col), arr) <- res) {
            val offset = (blk_row * ROWS_PER_BLK.toLong, blk_col * COLS_PER_BLK.toLong)
            for (((i, j), v) <- arr) {
                if (q.size < k) {
                    q.enqueue(((offset._1 + i, offset._2 + j), v))
                }
                else {
                    q.enqueue(((offset._1 + i, offset._2 + j), v))
                    q.dequeue()
                }
            }
        }
        q.toArray
    }

    private def orderByValueInt(t: ((Int, Int), Double)) = {
        - t._2
    }

    private def orderByValueDouble(t: ((Long, Long), Double)) = {
        - t._2
    }

    // duplicate the matrix for parNum copies and distribute them over the cluster
    private def duplicateCrossPartitions(rdd: RDD[MatrixBlk], parNum: Int): RDD[(Int, Iterator[MatrixBlk])] = {
        rdd.flatMap{ case ((row, col), mat) =>
            val arr = new Array[(Int, MatrixBlk)](parNum)
            for (i <- 0 until parNum) {
                arr(i) = (i, ((row, col), mat))
            }
            arr
        }.groupByKey(new IndexPartitioner(parNum))
        .mapValues(iter => iter.toIterator)
    }

    def blockMultiplyDup(other: BlockPartitionMatrix): BlockPartitionMatrix = {
        require(nCols() == other.nRows(), s"#cols of A should be equal to #rows of B, but found " +
          s"A.numCols = ${nCols()}, B.numRows = ${other.nRows()}")
        var rddB = other.blocks
        //var useOtherMatrix: Boolean = false
        if (COLS_PER_BLK != other.ROWS_PER_BLK) {
            logWarning(s"Repartition Matrix B since A.col_per_blk = $COLS_PER_BLK and B.row_per_blk = ${other.ROWS_PER_BLK}")
            rddB = getNewBlocks(other.blocks, other.ROWS_PER_BLK, other.COLS_PER_BLK,
                COLS_PER_BLK, COLS_PER_BLK, new RowPartitioner(numPartitions))
            //useOtherMatrix = true
        }

        val otherDup = duplicateCrossPartitions(rddB, blocks.partitions.length)

        val OTHER_COL_BLK_NUM = math.ceil(other.nCols() * 1.0 / COLS_PER_BLK).toInt
        val resultPartitioner = BlockCyclicPartitioner(ROW_BLK_NUM, OTHER_COL_BLK_NUM, numPartitions)

        val partRDD = blocks.mapPartitionsWithIndex { (idx, iter) =>
            Iterator((idx, iter.toIndexedSeq.iterator))
        }
        //println("mapPartitionsWithIndex: " + partRDD.count())
        val prodRDD = partRDD
          .join(otherDup)
          .flatMap { case (pidx, (iter1, iter2)) =>
            // aggregate local block matrices on each partition
            val idxToMat = new TrieMap[(Int, Int), MLMatrix]()
            val iter2Dup = iter2.toSeq
            for (blk1 <- iter1) {
                for (blk2 <- iter2Dup) {
                    if (blk1._1._2 == blk2._1._1) {
                        val key = (blk1._1._1, blk2._1._2)
                        if (!idxToMat.contains(key)) {
                            val prod = LocalMatrix.matrixMultiplication(blk1._2, blk2._2)
                            idxToMat.putIfAbsent(key, prod)
                        }
                        else {
                            val prod1 = idxToMat.get(key)
                            val prod2 = LocalMatrix.add(prod1.get, LocalMatrix.matrixMultiplication(blk1._2, blk2._2))
                            idxToMat.replace(key, prod2)
                        }
                    }
                }
            }
            idxToMat.iterator
        }.reduceByKey(resultPartitioner, LocalMatrix.add(_, _))
        new BlockPartitionMatrix(prodRDD, ROWS_PER_BLK, COLS_PER_BLK, nRows(), other.nCols())
    }

    def frobenius(): Double = {
        val t = blocks.map { case ((i, j), mat) =>
            val x = LocalMatrix.frobenius(mat)
            x * x
        }.reduce(_ + _)
        math.sqrt(t)
    }
}

object BlockPartitionMatrix {
    // TODO: finish some helper factory methods
    def createFromCoordinateEntries(entries: RDD[Entry],
                                    ROWS_PER_BLK: Int,
                                    COLS_PER_BLK: Int,
                                    ROW_NUM: Long = 0,
                                    COL_NUM: Long = 0): BlockPartitionMatrix = {
        require(ROWS_PER_BLK > 0, s"ROWS_PER_BLK needs to be greater than 0. " +
        s"But found ROWS_PER_BLK = $ROWS_PER_BLK")
        require(COLS_PER_BLK > 0, s"COLS_PER_BLK needs to be greater than 0. " +
        s"But found COLS_PER_BLK = $COLS_PER_BLK")
        var colSize = entries.map(x => x.col).max() + 1
        if (COL_NUM > 0 && colSize > COL_NUM) {
            println(s"Computing colSize is greater than COL_NUM, colSize = $colSize, COL_NUM = $COL_NUM")
        }
        if (COL_NUM > colSize) colSize = COL_NUM
        var rowSize = entries.map(x => x.row).max() + 1
        if (ROW_NUM > 0 && rowSize > ROW_NUM) {
            println(s"Computing rowSize is greater than ROW_NUM, rowSize = $rowSize, ROW_NUM = $ROW_NUM")
        }
        if (ROW_NUM > rowSize) rowSize = ROW_NUM
        val ROW_BLK_NUM = math.ceil(rowSize * 1.0 / ROWS_PER_BLK).toInt
        val COL_BLK_NUM = math.ceil(colSize * 1.0 / COLS_PER_BLK).toInt
        //val partitioner = BlockCyclicPartitioner(ROW_BLK_NUM, COL_BLK_NUM, entries.partitions.length)
        //val partitioner = new ColumnPartitioner(8)
        val blocks: RDD[((Int, Int), MLMatrix)] = entries.map { entry =>
            val blkRowIdx = (entry.row / ROWS_PER_BLK).toInt
            val blkColIdx = (entry.col / COLS_PER_BLK).toInt
            val rowId = entry.row % ROWS_PER_BLK
            val colId = entry.col % COLS_PER_BLK
            ((blkRowIdx, blkColIdx), (rowId.toInt, colId.toInt, entry.value))
        }.groupByKey().map { case ((blkRowIdx, blkColIdx), entry) =>
            val effRows = math.min(rowSize - blkRowIdx.toLong * ROWS_PER_BLK, ROWS_PER_BLK).toInt
            val effCols = math.min(colSize - blkColIdx.toLong * COLS_PER_BLK, COLS_PER_BLK).toInt
            ((blkRowIdx, blkColIdx), SparseMatrix.fromCOO(effRows, effCols, entry))
        }
        new BlockPartitionMatrix(blocks, ROWS_PER_BLK, COLS_PER_BLK, rowSize, colSize)
    }

    def PageRankMatrixFromCoordinateEntries(entries: RDD[Entry],
                                            ROWS_PER_BLK: Int,
                                            COLS_PER_BLK: Int): BlockPartitionMatrix = {
        require(ROWS_PER_BLK > 0, s"ROWS_PER_BLK needs to be greater than 0. " +
        s"But found ROWS_PER_BLK = $ROWS_PER_BLK")
        require(COLS_PER_BLK > 0, s"COLS_PER_BLK needs to be greater than 0. " +
        s"But found COLS_PER_BLK = $COLS_PER_BLK")
        val rowSize = entries.map(x => x.row).max() + 1
        val colSize = entries.map(x => x.col).max() + 1
        val size = math.max(rowSize, colSize)   // make sure the generating matrix is a square matrix
        val wRdd = entries.map(entry => (entry.row, entry))
        .groupByKey().map { x =>
            (x._1, 1.0 / x._2.size)
        }
        val prEntries = entries.map { entry =>
            (entry.row, entry)
        }.join(wRdd)
        .map { record =>
            val rid = record._2._1.col
            val cid = record._2._1.row
            val v = record._2._1.value * record._2._2
            Entry(rid, cid, v)
        }
        createFromCoordinateEntries(prEntries, ROWS_PER_BLK, COLS_PER_BLK, size, size)
    }

    def onesMatrixList(nrows: Long, ncols: Long, ROWS_PER_BLK: Int, COLS_PER_BLK: Int): List[((Int, Int), MLMatrix)] = {
        val ROW_BLK_NUM = math.ceil(nrows * 1.0 / ROWS_PER_BLK).toInt
        val COL_BLK_NUM = math.ceil(ncols * 1.0 / COLS_PER_BLK).toInt
        var res = scala.collection.mutable.LinkedList[((Int, Int), MLMatrix)]()
        for (i <- 0 until ROW_BLK_NUM; j <- 0 until COL_BLK_NUM) {
            val rowSize = math.min(ROWS_PER_BLK, nrows - i * ROWS_PER_BLK).toInt
            val colSize = math.min(COLS_PER_BLK, ncols - j * COLS_PER_BLK).toInt
            res = res :+ ((i, j), DenseMatrix.ones(rowSize, colSize))
        }
        res.toList
    }
    // estimate a proper block size
    def estimateBlockSize(rdd: RDD[Entry]): Int = {
        val nrows = rdd.map(entry => entry.row)
          .distinct()
          .count()
        val ncols = rdd.map(entry => entry.col)
          .distinct()
          .count()
        // get system parameters
        val numWorkers = rdd.context.getExecutorStorageStatus.length - 1
        println(s"numWorkers = $numWorkers")
        val coresPerWorker = 12
        //println(s"coresPerWorker = $coresPerWorker")
        var blkSize = Math.sqrt(nrows * ncols / (numWorkers * coresPerWorker)).toInt
        blkSize = blkSize - (blkSize % 100) + 100
        blkSize
    }
}

object TestBlockPartition {
    def main (args: Array[String]) {
        val conf = new SparkConf()
          .setMaster("local[4]")
          .setAppName("Test for block partition matrices")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.shuffle.consolidateFiles", "true")
          .set("spark.shuffle.compress", "false")
        val sc = new SparkContext(conf)
        // the following test the block matrix addition and multiplication
        val m1 = new DenseMatrix(3,3,Array[Double](1,7,13,2,8,14,3,9,15))
        val m2 = new DenseMatrix(3,3,Array[Double](4,10,16,5,11,17,6,12,18))
        val m3 = new DenseMatrix(3,3,Array[Double](19,25,31,20,26,32,21,27,33))
        val m4 = new DenseMatrix(3,3,Array[Double](22,28,34,23,29,35,24,30,36))
        val n1 = new DenseMatrix(4,4,Array[Double](1,1,1,3,1,1,1,3,1,1,1,3,2,2,2,4))
        val n2 = new DenseMatrix(4,2,Array[Double](2,2,2,4,2,2,2,4))
        val n3 = new DenseMatrix(2,4,Array[Double](3,3,3,3,3,3,4,4))
        val n4 = new DenseMatrix(2,2,Array[Double](4,4,4,4))
        val arr1 = Array[((Int, Int), MLMatrix)](((0,0),m1), ((0,1), m2), ((1,0), m3), ((1,1), m4))
        val arr2 = Array[((Int, Int), MLMatrix)](((0,0),n1), ((0,1), n2), ((1,0), n3), ((1,1), n4))
        val rdd1 = sc.parallelize(arr1, 2)
        val rdd2 = sc.parallelize(arr2, 2)
        val mat1 = new BlockPartitionMatrix(rdd1, 3, 3, 6, 6)
        val mat2 = new BlockPartitionMatrix(rdd2, 4, 4, 6, 6)
        //println(mat1.add(mat2).toLocalMatrix())
        /*  addition
         *  2.0   3.0   4.0   6.0   7.0   8.0
            8.0   9.0   10.0  12.0  13.0  14.0
            14.0  15.0  16.0  18.0  19.0  20.0
            22.0  23.0  24.0  26.0  27.0  28.0
            28.0  29.0  30.0  32.0  33.0  34.0
            34.0  35.0  36.0  38.0  39.0  40.0
         */
        println((mat1 %*% mat2).toLocalMatrix())
        /*   multiplication
             51    51    51    72    72    72
             123   123   123   180   180   180
             195   195   195   288   288   288
             267   267   267   396   396   396
             339   339   339   504   504   504
             411   411   411   612   612   612
         */


        /*val mat = List[(Long, Long)]((0, 0), (0,1), (0,2), (0,3), (0, 4), (0, 5), (1, 0), (1, 2),
            (2, 3), (2, 4), (3,1), (3,2), (3, 4), (4, 5), (5, 4))
        val CooRdd = sc.parallelize(mat, 2).map(x => Entry(x._1, x._2, 1.0))
        var matrix = BlockPartitionMatrix.PageRankMatrixFromCoordinateEntries(CooRdd, 3, 3).cache()
        val vec = BlockPartitionMatrix.onesMatrixList(6, 1, 3, 3)//List[((Int, Int), MLMatrix)](((0, 0), DenseMatrix.ones(3, 1)), ((1, 0), DenseMatrix.ones(3, 1)))
        val vecRdd = sc.parallelize(vec, 2)
        var x = new BlockPartitionMatrix(vecRdd, 3, 3, 6, 1).multiplyScalar(1.0 / 6)
        var v = new BlockPartitionMatrix(vecRdd, 3, 3, 6, 1).multiplyScalar(1.0 / 6)
        val alpha = 0.85
        matrix = (alpha *: matrix).partitionByBlockCyclic().cache()
        v = (1.0 - alpha) *: v
        for (i <- 0 until 10) {
            x = (matrix %*% x) + (v, (3,3))
            //x = matrix.multiply(x).multiplyScalar(alpha).add(v.multiplyScalar(1-alpha), (3,3))
        }
        println(x.toLocalMatrix())*/
        sc.stop()
    }
}