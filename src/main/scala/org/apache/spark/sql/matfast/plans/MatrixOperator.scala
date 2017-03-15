package org.apache.spark.sql.matfast.plans

import org.apache.spark.sql.catalyst.expressions.{Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, UnaryNode}

/**
  * Created by yongyangyu on 2/23/17.
  */
case class TranposeOperator(child: LogicalPlan) extends UnaryNode {
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