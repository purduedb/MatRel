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

package org.apache.spark.sql.matfast

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.{Encoder, Row, Dataset => SQLDataSet}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.matfast
import org.apache.spark.sql.matfast.plans._


class Dataset[T] private[matfast]
(@transient val matfastSession: matfast.MatfastSession,
 @transient override val queryExecution: QueryExecution,
 encoder: Encoder[T]) extends SQLDataSet[T](matfastSession, queryExecution.logical, encoder)
{
  def this(sparkSession: matfast.MatfastSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(sparkSession, sparkSession.sessionState.executePlan(logicalPlan), encoder)
  }

  def selectRow(nrows: Long, ncols: Long, blkSize: Int, index: Long,
                 data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    require(index <= nrows && index >= 1,
      s"row index should be smaller than #rows, index=$index, #rows=$nrows")
    SelectOperator(this.logicalPlan, nrows, ncols, blkSize, true, index - 1)
  }

  def selectColumn(nrows: Long, ncols: Long, blkSize: Int, index: Long,
                    data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    require(index <= ncols &&  index >= 1,
      s"col index should be smaller than #cols, index=$index, #cols=$ncols")
    SelectOperator(this.logicalPlan, nrows, ncols, blkSize, false, index - 1)
  }

  def selectCell(nrows: Long, ncols: Long, blkSize: Int,
             rowIdx: Long, colIdx: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    require(rowIdx <= nrows && rowIdx >= 1,
      s"row index should be smaller than #rows, rid=$rowIdx, #rows=$nrows")
    require(colIdx <= ncols && colIdx >= 1,
      s"col index should be smaller than #cols, cid=$colIdx, #cols=$ncols")
    SelectCellOperator(this.logicalPlan, nrows, ncols, blkSize, rowIdx - 1, colIdx - 1)
  }

  def t(): DataFrame = transpose()

  def transpose(data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    TransposeOperator(this.logicalPlan)
  }

  def rowSum(nrows: Long, ncols: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    RowSumOperator(this.logicalPlan, nrows, ncols)
  }

  def colSum(nrows: Long, ncols: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    ColumnSumOperator(this.logicalPlan, nrows, ncols)
  }

  def sum(nrows: Long, ncols: Long,
          data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    SumOperator(this.logicalPlan, nrows, ncols)
  }

  def trace(nrows: Long, ncols: Long,
            data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    require(nrows == ncols, s"Cannot perform trace() on a rectangle matrix")
    TraceOperator(this.logicalPlan, nrows, ncols)
  }

  def rowNnz(nrows: Long, ncols: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    RowNnzOperator(this.logicalPlan, nrows, ncols)
  }

  def colNnz(nrows: Long, ncols: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    ColumnNnzOperator(this.logicalPlan, nrows, ncols)
  }

  def nnz(nrows: Long, ncols: Long,
          data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    NnzOperator(this.logicalPlan, nrows, ncols)
  }

  def diagNnz(nrows: Long, ncols: Long,
              data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    require(nrows == ncols, s"Cannot perform diagNnz() on a rectangle matrix")
    DiagNnzOperator(this.logicalPlan, nrows, ncols)
  }

  def rowAvg(nrows: Long, ncols: Long, blkSize: Int,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    RowAvgOperator(this.logicalPlan, nrows, ncols, blkSize)
  }

  def colAvg(nrows: Long, ncols: Long, blkSize: Int,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    ColumnAvgOperator(this.logicalPlan, nrows, ncols, blkSize)
  }

  def avg(nrows: Long, ncols: Long, blkSize: Int,
          data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    AvgOperator(this.logicalPlan, nrows, ncols, blkSize)
  }

  def diagAvg(nrows: Long, ncols: Long, blkSize: Int,
              data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    DiagAvgOperator(this.logicalPlan, nrows, ncols, blkSize)
  }

  def rowMax(nrows: Long, ncols: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    RowMaxOperator(this.logicalPlan, nrows, ncols)
  }

  def colMax(nrows: Long, ncols: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    ColumnMaxOperator(this.logicalPlan, nrows, ncols)
  }

  def max(nrows: Long, ncols: Long,
          data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    MaxOperator(this.logicalPlan, nrows, ncols)
  }

  def diagMax(nrows: Long, ncols: Long,
              data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    DiagMaxOperator(this.logicalPlan, nrows, ncols)
  }

  def rowMin(nrows: Long, ncols: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    RowMinOperator(this.logicalPlan, nrows, ncols)
  }

  def colMin(nrows: Long, ncols: Long,
             data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    ColumnMinOperator(this.logicalPlan, nrows, ncols)
  }

  def diagMin(nrows: Long, ncols: Long,
              data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    DiagMinOperator(this.logicalPlan, nrows, ncols)
  }

  def selectValue(v: Double, eps: Double = 1e-6,
                  data: Seq[Attribute] =
                    this.queryExecution.analyzed.output): DataFrame = withPlan {
    SelectCellValueOperator(this.logicalPlan, v, eps)
  }

  def removeEmptyRows(data: Seq[Attribute] =
                      this.queryExecution.analyzed.output): DataFrame = withPlan {
    RemoveEmptyRowsOperator(this.logicalPlan)
  }

  def removeEmptyColumns(data: Seq[Attribute] =
                        this.queryExecution.analyzed.output): DataFrame = withPlan {
    RemoveEmptyColumnsOperator(this.logicalPlan)
  }

  def vec(nrows: Long, ncols: Long, blkSize: Int,
          data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    VectorizeOperator(this.logicalPlan, nrows, ncols, blkSize)
  }

  def addScalar(alpha: Double,
                data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan { MatrixScalarAddOperator(this.logicalPlan, alpha) }

  def multiplyScalar(alpha: Double,
                     data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    MatrixScalarMultiplyOperator(this.logicalPlan, alpha)
  }

  def power(alpha: Double,
            data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    MatrixPowerOperator(this.logicalPlan, alpha)
  }

  def addElement(leftRowNum: Long, leftColNum: Long,
                 right: Dataset[_],
                 rightRowNum: Long, rightColNum: Long,
                 blkSize: Int,
                 data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    MatrixElementAddOperator(this.logicalPlan, leftRowNum, leftColNum,
      right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  def multiplyElement(leftRowNum: Long, leftColNum: Long,
                      right: Dataset[_],
                      rightRowNum: Long, rightColNum: Long,
                      blkSize: Int,
                      data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    MatrixElementMultiplyOperator(this.logicalPlan, leftRowNum, leftColNum,
      right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  def divideElement(leftRowNum: Long, leftColNum: Long,
                    right: Dataset[_],
                    rightRowNum: Long, rightColNum: Long,
                    blkSize: Int,
                    data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    MatrixElementDivideOperator(this.logicalPlan, leftRowNum, leftColNum,
      right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  def matrixMultiply(leftRowNum: Long, leftColNum: Long,
                     right: Dataset[_],
                     rightRowNum: Long, rightColNum: Long,
                     blkSize: Int,
                     data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    MatrixMatrixMultiplicationOperator(this.logicalPlan, leftRowNum, leftColNum,
      right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  def matrixRankOneUpdate(leftRowNum: Long, leftColNum: Long,
                          right: Dataset[_],
                          rightRowNum: Long, rightColNum: Long,
                          blkSize: Int,
                          data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    RankOneUpdateOperator(this.logicalPlan, leftRowNum, leftColNum,
      right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  // A joinTwoIndices() operator on two matrices, A and B, where
  // A is an m-by-n matrix, and B is a p-by-q matrix returns a matrix,
  // whose dimension is min(m, p)-by-min(n, q), and cell value is defined
  // by the merge function. If any input cell is empty, the merge function
  // should return an empty value for the cell.
  // By default, the empty value should be represented by 0.
  def joinTwoIndices(leftRowNum: Long, leftColNum: Long,
                     right: Dataset[_],
                     rightRowNum: Long, rightColNum: Long,
                     mergeFunc: (Double, Double) => Double,
                     blkSize: Int,
                     isSwapped: Boolean = false,
                     data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    require(leftColNum > blkSize && leftRowNum > blkSize,
      s"left matrix size is smaller than blkSize")
    require(rightRowNum > blkSize && rightColNum > blkSize,
      s"right matrix size is smaller than blkSize")
    JoinTwoIndicesOperator(this.logicalPlan, leftRowNum, leftColNum,
      right.logicalPlan, rightRowNum, rightColNum, mergeFunc, blkSize, isSwapped)
  }

  // This crossProduct() should be used with great caution, as it may blow the cluster memory.
  def crossProduct(leftRowNum: Long, leftColNum: Long, isLeftSpase: Boolean,
                   right: Dataset[_],
                   rightRowNum: Long, rightColNum: Long, isRightSparse: Boolean,
                   mergeFunc: (Double, Double) => Double,
                   blkSize: Int,
                   data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    CrossProductOperator(this.logicalPlan, leftRowNum, leftColNum, isLeftSpase,
      right.logicalPlan, rightRowNum, rightColNum, isRightSparse, mergeFunc, blkSize)
  }

  def joinOnValues(leftRowNum: Long, leftColNum: Long,
                   right: Dataset[_],
                   rightRowNum: Long, rightColNum: Long,
                   mergeFunc: (Double, Double) => Double,
                   blkSize: Int,
                   data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    JoinOnValuesOperator(this.logicalPlan, leftRowNum, leftColNum,
      right.logicalPlan, rightRowNum, rightColNum, mergeFunc, blkSize)
  }

  def joinIndexValue(leftRowNum: Long, leftColNum: Long,
                     right: Dataset[_],
                     rightRowNum: Long, rightColNum: Long,
                     mode: Int,
                     mergeFunc: (Double, Double) => Double,
                     blkSize: Int,
                     data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    JoinIndexValueOperator(this.logicalPlan, leftRowNum, leftColNum,
      right.logicalPlan, rightRowNum, rightColNum, mode, mergeFunc, blkSize)
  }

  def joinOnSingleIndex(leftRowNum: Long, leftColNum: Long, isLeftSparse: Boolean,
                  right: Dataset[_],
                  rightRowNum: Long, rightColNum: Long, isRightSparse: Boolean,
                  mode: Int,
                  mergeFunc: (Double, Double) => Double,
                  blkSize: Int,
                  data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    JoinIndexOperator(this.logicalPlan, leftRowNum, leftColNum, isLeftSparse,
      right.logicalPlan, rightRowNum, rightColNum, isRightSparse, mode, mergeFunc, blkSize)
  }

  def groupBy4DTensor(dims: Int, aggFunc: (Double, Double) => Double,
                      data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame
  = withPlan {
    GroupBy4DTensorOperator(this.logicalPlan, dims, aggFunc)
  }

  private def getAttributes(keys: Array[String],
                            attrs: Seq[Attribute] =
                            this.queryExecution.analyzed.output): Array[Attribute] = {
    keys.map(key => {
      val tmp = attrs.indexWhere(_.name == key)
      if (tmp >= 0) attrs(tmp)
      else null
    })
  }

  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    Dataset.ofRows(matfastSession, logicalPlan)
  }
}

private[matfast] object Dataset {
  def apply[T: Encoder](sparkSession: matfast.MatfastSession,
                        logicalPlan: LogicalPlan): Dataset[T] = {
    new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
  }

  def ofRows(sparkSession: matfast.MatfastSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
}


















