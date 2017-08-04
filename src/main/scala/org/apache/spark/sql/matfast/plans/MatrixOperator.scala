package org.apache.spark.sql.matfast.plans

import org.apache.spark.sql.catalyst.expressions.{Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, UnaryNode}

/**
  * Created by yongyangyu on 2/23/17.
  */
case class TranposeOperator(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

// Computes sum along the row direction, resulting in a column vector
// The schema of the following three operators are correct. Everything is deemed as a matrix.
case class RowSumOperator(child: LogicalPlan,
                          nrows: Long,
                          ncols: Long) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

// Computes sum along the column direction, resulting in a row vector
case class ColumnSumOperator(child: LogicalPlan,
                             nrows: Long,
                             ncols: Long) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

// Computes sum of all the elements in a matrix, resulting in a scalar
case class SumOperator(child: LogicalPlan,
                       nrows: Long,
                       ncols: Long) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class MatrixScalarAddOperator(child: LogicalPlan, scalar: Double) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class MatrixScalarMultiplyOperator(child: LogicalPlan, scalar: Double) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class MatrixPowerOperator(child: LogicalPlan, scalar: Double) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

// stack the elements of a matrix in a column fashion
case class VectorizeOperator(child: LogicalPlan, nrows: Long, ncols: Long, blkSize: Int) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class MatrixElementAddOperator(leftChild: LogicalPlan,
                                    leftRowNum: Long,
                                    leftColNum: Long,
                                    rightChild: LogicalPlan,
                                    rightRowNum: Long,
                                    rightColNum: Long,
                                    blkSize: Int) extends BinaryNode {
  override def output: Seq[Attribute] = leftChild.output

  override def left: LogicalPlan = leftChild

  override def right: LogicalPlan = rightChild
}

case class MatrixElementMultiplyOperator(leftChild: LogicalPlan,
                                        leftRowNum: Long,
                                        leftColNum: Long,
                                        rightChild: LogicalPlan,
                                        rightRowNum: Long,
                                        rightColNum: Long,
                                        blkSize: Int) extends BinaryNode {
  override def output: Seq[Attribute] = leftChild.output

  override def left: LogicalPlan = leftChild

  override def right: LogicalPlan = rightChild
}

case class MatrixElementDivideOperator(leftChild: LogicalPlan,
                                       leftRowNum: Long,
                                       leftColNum: Long,
                                       rightChild: LogicalPlan,
                                       rightRowNum: Long,
                                       rightColNum: Long,
                                       blkSize: Int) extends BinaryNode {
  override def output: Seq[Attribute] = leftChild.output

  override def left: LogicalPlan = leftChild

  override def right: LogicalPlan = rightChild
}

case class MatrixMatrixMultiplicationOperator(leftChild: LogicalPlan,
                                              leftRowNum: Long,
                                              leftColNum: Long,
                                              rightChild: LogicalPlan,
                                              rightRowNum: Long,
                                              rightColNum: Long,
                                              blkSize: Int) extends BinaryNode {
  override def output: Seq[Attribute] = leftChild.output

  override def left: LogicalPlan = leftChild

  override def right: LogicalPlan = rightChild
}

// this class implements rank-1 update for an existing matrix
// without explicitly materializing the intermediate matrix of vector outer-product
case class RankOneUpdateOperator(leftChild: LogicalPlan,
                                 leftRowNum: Long,
                                 leftColNum: Long,
                                 rightChild: LogicalPlan,
                                 rightRowNum: Long,
                                 rightColNum: Long,
                                 blkSize: Int) extends BinaryNode {

  override def output: Seq[Attribute] = leftChild.output

  override def left: LogicalPlan = leftChild

  override def right: LogicalPlan = rightChild
}