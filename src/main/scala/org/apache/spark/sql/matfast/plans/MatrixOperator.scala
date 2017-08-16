/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.matfast.plans

import org.apache.spark.sql.catalyst.expressions.{Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, UnaryNode}

// Project a row or column from a given matrix
// rowOrCol: true -- project a row; otherwise, project a column
case class ProjectOperator(child: LogicalPlan,
                           nrows: Long,
                           ncols: Long,
                           blkSize: Int,
                           rowOrCol: Boolean,
                           index: Long) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class SelectOperator(child: LogicalPlan,
                          nrows: Long,
                          ncols: Long,
                          blkSize: Int,
                          rowIdx: Long,
                          colIdx: Long) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class TransposeOperator(child: LogicalPlan) extends UnaryNode {
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

// Computes trace of the matrix, i.e., compute the summation of the diagonal of a square matrix
case class TraceOperator(child: LogicalPlan,
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
case class VectorizeOperator(child: LogicalPlan, nrows: Long,
                             ncols: Long, blkSize: Int) extends UnaryNode {
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