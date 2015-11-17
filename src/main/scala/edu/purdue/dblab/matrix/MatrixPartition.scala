package edu.purdue.dblab.matrix

/**
 * Created by yongyangyu on 6/16/15.
 * MatrixPartition serves as an indicator for partitioning schemes of matrices.
 */
object MatrixPartition extends Enumeration{
    type MatrixPartition = Value
    val ROW, COL, BLK = Value
}

object Main extends App {
    import MatrixPartition._
    def show(par: MatrixPartition) = {
        if (par == ROW) {
            println("row partitioned matrix")
        }
        else if (par == COL) {
            println("column partitioned matrix")
        }
        else if (par == BLK) {
            println("block partitioned matrix")
        }
        else {
            println("partition scheme undefined")
        }
    }
    MatrixPartition.values.foreach(show)
}