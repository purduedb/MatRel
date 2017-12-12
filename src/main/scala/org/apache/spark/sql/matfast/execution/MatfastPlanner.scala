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

package org.apache.spark.sql.matfast.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{catalyst, Strategy}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.matfast.MatfastSession
import org.apache.spark.sql.matfast.execution._
import org.apache.spark.sql.matfast.plans._


class MatfastPlanner(val matfastContext: MatfastSession,
                     override val conf: SQLConf,
                     override val extraStrategies: Seq[Strategy])
  extends SparkPlanner(matfastContext.sparkContext, conf, extraStrategies) {

  override def strategies: Seq[Strategy] =
    (MatrixOperators :: Nil) ++ super.strategies
}

object MatrixOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ProjectOperator(child, nrows, ncols, blkSize, rowOrCol, index) =>
      if (rowOrCol) {
        child match {
          case TransposeOperator(ch) =>
            ProjectColumnDirectExecution(planLater(ch), blkSize, index) :: Nil
          case MatrixScalarAddOperator(ch, alpha) =>
            MatrixScalarAddExecution(planLater(
              ProjectOperator(ch, nrows, ncols, blkSize, rowOrCol, index)), alpha) :: Nil
          case MatrixScalarMultiplyOperator(ch, alpha) =>
            MatrixScalarMultiplyExecution(planLater(
              ProjectOperator(ch, nrows, ncols, blkSize, rowOrCol, index)), alpha) :: Nil
          case MatrixElementAddOperator(left, leftRowNum, leftColNum,
          right, rightRowNum, rightColNum, blkSize) =>
            MatrixElementAddExecution(planLater(
              ProjectOperator(left, leftRowNum, leftColNum, blkSize, rowOrCol, index)),
              leftRowNum, leftColNum, planLater(
              ProjectOperator(right, rightRowNum, rightColNum, blkSize, rowOrCol, index)),
              rightRowNum, rightColNum, blkSize) :: Nil
          case MatrixElementMultiplyOperator(left, leftRowNum, leftColNum,
          right, rightRowNum, rightColNum, blkSize) =>
            MatrixElementMultiplyExecution(planLater(
              ProjectOperator(left, leftRowNum, leftColNum, blkSize, rowOrCol, index)),
              leftRowNum, leftColNum, planLater(
              ProjectOperator(right, rightRowNum, rightColNum, blkSize, rowOrCol, index)),
              rightRowNum, rightColNum, blkSize) :: Nil
          case MatrixElementDivideOperator(left, leftRowNum, leftColNum,
          right, rightRowNum, rightColNum, blkSize) =>
            MatrixElementDivideExecution(planLater(
              ProjectOperator(left, leftRowNum, leftColNum, blkSize, rowOrCol, index)),
              leftRowNum, leftColNum, planLater(
              ProjectOperator(right, rightRowNum, rightColNum, blkSize, rowOrCol, index)),
              rightRowNum, rightColNum, blkSize) :: Nil
          case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum,
          right, rightRowNum, rightColNum, blkSize) =>
            MatrixMatrixMultiplicationExecution(planLater(
              ProjectOperator(left, leftRowNum, leftColNum, blkSize, rowOrCol, index)),
              leftRowNum, leftColNum, planLater(right),
              rightRowNum, rightColNum, blkSize) :: Nil
          case _ =>
            ProjectRowDirectExecution(planLater(child), blkSize, index) :: Nil
        }
      } else {
        child match {
          case TransposeOperator(ch) =>
            ProjectRowDirectExecution(planLater(ch), blkSize, index) :: Nil
          case MatrixScalarAddOperator(ch, alpha) =>
            MatrixScalarAddExecution(planLater(
              ProjectOperator(ch, nrows, ncols, blkSize, rowOrCol, index)), alpha) :: Nil
          case MatrixScalarMultiplyOperator(ch, alpha) =>
            MatrixScalarMultiplyExecution(planLater(
              ProjectOperator(ch, nrows, ncols, blkSize, rowOrCol, index)), alpha) :: Nil
          case MatrixElementAddOperator(left, leftRowNum, leftColNum,
          right, rightRowNum, rightColNum, blkSize) =>
            MatrixElementAddExecution(planLater(
              ProjectOperator(left, leftRowNum, leftColNum, blkSize, rowOrCol, index)),
              leftRowNum, leftColNum, planLater(
                ProjectOperator(right, rightRowNum, rightColNum, blkSize, rowOrCol, index)),
              rightRowNum, rightColNum, blkSize) :: Nil
          case MatrixElementMultiplyOperator(left, leftRowNum, leftColNum,
          right, rightRowNum, rightColNum, blkSize) =>
            MatrixElementMultiplyExecution(planLater(
              ProjectOperator(left, leftRowNum, leftColNum, blkSize, rowOrCol, index)),
              leftRowNum, leftColNum, planLater(
                ProjectOperator(right, rightRowNum, rightColNum, blkSize, rowOrCol, index)),
              rightRowNum, rightColNum, blkSize) :: Nil
          case MatrixElementDivideOperator(left, leftRowNum, leftColNum,
          right, rightRowNum, rightColNum, blkSize) =>
            MatrixElementDivideExecution(planLater(
              ProjectOperator(left, leftRowNum, leftColNum, blkSize, rowOrCol, index)),
              leftRowNum, leftColNum, planLater(
                ProjectOperator(right, rightRowNum, rightColNum, blkSize, rowOrCol, index)),
              rightRowNum, rightColNum, blkSize) :: Nil
          case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum,
          right, rightRowNum, rightColNum, blkSize) =>
            MatrixMatrixMultiplicationExecution(planLater(left),
              leftRowNum, leftColNum, planLater(
              ProjectOperator(right, rightRowNum, rightColNum, blkSize, rowOrCol, index)),
              rightRowNum, rightColNum, blkSize) :: Nil
          case _ =>
            ProjectColumnDirectExecution(planLater(child), blkSize, index) :: Nil
        }
      }
    case ProjectCellOperator(child, nrows, ncols, blkSize, rowIdx, colIdx) =>
      child match {
        case TransposeOperator(ch) =>
          SelectDirectExecution(planLater(ch), blkSize, colIdx, rowIdx) :: Nil
        case MatrixScalarAddOperator(ch, alpha) =>
          MatrixScalarAddExecution(planLater(
            ProjectCellOperator(ch, nrows, ncols, blkSize, rowIdx, colIdx)), alpha) :: Nil
        case MatrixScalarMultiplyOperator(ch, alpha) =>
          MatrixScalarMultiplyExecution(planLater(
            ProjectCellOperator(ch, nrows, ncols, blkSize, rowIdx, colIdx)), alpha) :: Nil
        case MatrixElementAddOperator(left, leftRowNum, leftColNum,
        right, rightRowNum, rightColNum, blkSize) =>
          MatrixElementAddExecution(planLater(
            ProjectCellOperator(left, leftRowNum, leftColNum, blkSize, rowIdx, colIdx)),
            leftRowNum, leftColNum, planLater(
            ProjectCellOperator(right, rightRowNum, rightColNum, blkSize, rowIdx, colIdx)),
            rightRowNum, rightColNum, blkSize) :: Nil
        case MatrixElementMultiplyOperator(left, leftRowNum, leftColNum,
        right, rightRowNum, rightColNum, blkSize) =>
          MatrixElementMultiplyExecution(planLater(
            ProjectCellOperator(left, leftRowNum, leftColNum, blkSize, rowIdx, colIdx)),
            leftRowNum, leftColNum, planLater(
            ProjectCellOperator(right, rightRowNum, rightColNum, blkSize, rowIdx, colIdx)),
            rightRowNum, rightColNum, blkSize) :: Nil
        case MatrixElementDivideOperator(left, leftRowNum, leftColNum,
        right, rightRowNum, rightColNum, blkSize) =>
          MatrixElementDivideExecution(planLater(
            ProjectCellOperator(left, leftRowNum, leftColNum, blkSize, rowIdx, colIdx)),
            leftRowNum, leftColNum, planLater(
            ProjectCellOperator(right, rightRowNum, rightColNum, blkSize, rowIdx, colIdx)),
            rightRowNum, rightColNum, blkSize) :: Nil
        case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum,
        right, rightRowNum, rightColNum, blkSize) =>
          MatrixMatrixMultiplicationExecution(planLater(
            ProjectOperator(left, leftRowNum, leftColNum, blkSize, true, rowIdx)),
            leftRowNum, leftColNum, planLater(
            ProjectOperator(right, rightRowNum, rightColNum, blkSize, false, colIdx)),
            rightRowNum, rightColNum, blkSize) :: Nil
        case _ =>
          SelectDirectExecution(planLater(child), blkSize, rowIdx, colIdx) :: Nil
      }
    case TransposeOperator(child) => MatrixTransposeExecution(planLater(child)) :: Nil
    case RowSumOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MatrixTransposeExecution(planLater(ColumnSumOperator(beforeTrans, ncols, nrows))) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(RowSumOperator(ch, nrows, ncols)),
          alpha * ncols) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        MatrixScalarMultiplyExecution(planLater(RowSumOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixElementAddOperator(left, leftRowNum, leftColNum,
      right, rightRowNum, rightColNum, blkSize) =>
        MatrixElementAddExecution(planLater(RowSumOperator(left, leftRowNum, leftColNum)),
          leftRowNum, 1L, planLater(RowSumOperator(right, rightRowNum, rightColNum)),
          rightRowNum, 1L, blkSize) :: Nil
      case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum,
      right, rightRowNum, rightColNum, blkSize) =>
        MatrixMatrixMultiplicationExecution(planLater(left), leftRowNum, leftColNum,
          planLater(RowSumOperator(right, rightRowNum, rightColNum)),
          rightRowNum, 1L, blkSize) :: Nil
      case _ => RowSumDirectExecution(planLater(child)) :: Nil // default on an given matrix input
    }
    case ColumnSumOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MatrixTransposeExecution(planLater(RowSumOperator(beforeTrans, ncols, nrows))) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(ColumnSumOperator(ch, nrows, ncols)),
          alpha * nrows) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        MatrixScalarMultiplyExecution(planLater(ColumnSumOperator(ch, nrows, ncols)),
          alpha) :: Nil
      case MatrixElementAddOperator(left, leftRowNum, leftColNum,
      right, rightRowNum, rightColNum, blkSize) =>
        MatrixElementAddExecution(planLater(ColumnSumOperator(left, leftRowNum, leftColNum)),
          1L, leftColNum, planLater(ColumnSumOperator(right, rightRowNum, rightColNum)),
          1L, rightColNum, blkSize) :: Nil
      case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum,
      right, rightRowNum, rightColNum, blkSize) =>
        MatrixMatrixMultiplicationExecution(planLater(
          ColumnSumOperator(left, leftRowNum, leftColNum)),
          1L, leftColNum, planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
      case _ => ColumnSumDirectExecution(planLater(child)) :: Nil
    }
    case SumOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) => SumDirectExecution(planLater(beforeTrans)) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(SumOperator(ch, nrows, ncols)),
          alpha * nrows * ncols) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        MatrixScalarMultiplyExecution(planLater(SumOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixElementAddOperator(left, leftRowNum, leftColNum,
      right, rightRowNum, rightColNum, blkSize) =>
        MatrixElementAddExecution(planLater(SumOperator(left, leftRowNum, leftColNum)),
          1L, 1L, planLater(SumOperator(right, rightRowNum, rightColNum)), 1L, 1L, blkSize) :: Nil
      case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum,
      right, rightRowNum, rightColNum, blkSize) =>
        MatrixMatrixMultiplicationExecution(planLater(
          ColumnSumOperator(left, leftRowNum, leftColNum)), 1L, leftColNum,
          planLater(RowSumOperator(right, rightRowNum, rightColNum)),
          rightRowNum, 1L, blkSize) :: Nil
      case _ => SumDirectExecution(planLater(child)) :: Nil
    }
    case TraceOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) => TraceDirectExecution(planLater(beforeTrans)) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(TraceOperator(ch, nrows, ncols)), alpha * nrows) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        MatrixScalarMultiplyExecution(planLater(TraceOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixElementAddOperator(left, leftRowNum, leftColNum,
      right, rightRowNum, rightColNum, blkSize) =>
        MatrixElementAddExecution(planLater(TraceOperator(left, leftRowNum, leftColNum)),
          1L, 1L, planLater(TraceOperator(right, rightRowNum, rightColNum)), 1L, 1L, blkSize) :: Nil
      case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum,
      right, rightRowNum, rightColNum, blkSize) =>
        SumDirectExecution(planLater(MatrixElementMultiplyOperator(TransposeOperator(left),
          leftColNum, leftRowNum, right, rightRowNum, rightColNum, blkSize))) :: Nil
      case _ => TraceDirectExecution(planLater(child)) :: Nil
    }
    case RowNnzOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MatrixTransposeExecution(planLater(ColumnNnzOperator(beforeTrans, ncols, nrows))) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        RowNnzOnesExecution(planLater(ch)) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        RowNnzDirectExecution(planLater(ch)) :: Nil
      case MatrixElementDivideOperator(leftChild, leftRowNum, leftColNum,
      rightChild, rightRowNum, rightColNum, blkSize) =>
        RowNnzDirectExecution(planLater(leftChild)) :: Nil
      case _ => RowNnzDirectExecution(planLater(child)) :: Nil
    }
    case ColumnNnzOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MatrixTransposeExecution(planLater(RowNnzOperator(beforeTrans, ncols, nrows))) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        ColumnNnzOnesExecution(planLater(ch)) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        ColumnNnzDirectExecution(planLater(ch)) :: Nil
      case MatrixElementDivideOperator(leftChild, leftRowNum, leftColNum,
      rightChild, rightRowNum, rightColNum, blkSize) =>
        ColumnNnzDirectExecution(planLater(leftChild)) :: Nil
      case _ => ColumnNnzDirectExecution(planLater(child)) :: Nil
    }
    case NnzOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        NnzDirectExecution(planLater(beforeTrans)) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        NnzOnesExecution(planLater(ch)) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        NnzDirectExecution(planLater(ch)) :: Nil
      case MatrixElementDivideOperator(leftChild, leftRowNum, leftColNum,
      rightChild, rightRowNum, rightColNum, blkSize) =>
        NnzDirectExecution(planLater(leftChild)) :: Nil
      case _ => NnzDirectExecution(planLater(child)) :: Nil
    }
    case DiagNnzOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        DiagNnzDirectExecution(planLater(beforeTrans)) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        DiagNnzOnesExecution(planLater(ch)) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        DiagNnzDirectExecution(planLater(ch)) :: Nil
      case MatrixElementDivideOperator(leftChild, leftRowNum, leftColNum,
      rightChild, rightRowNum, rightColNum, blkSize) =>
        DiagNnzDirectExecution(planLater(leftChild)) :: Nil
      case _ => DiagNnzDirectExecution(planLater(child)) :: Nil
    }
    case RowAvgOperator(child, nrows, ncols, blkSize) =>
      MatrixElementDivideExecution(planLater(RowSumOperator(child, nrows, ncols)),
        nrows, 1, planLater(RowNnzOperator(child, nrows, ncols)), nrows, 1, blkSize) :: Nil
    case ColumnAvgOperator(child, nrows, ncols, blkSize) =>
      MatrixElementDivideExecution(planLater(ColumnSumOperator(child, nrows, ncols)),
        1, ncols, planLater(ColumnNnzOperator(child, nrows, ncols)), 1, ncols, blkSize) :: Nil
    case AvgOperator(child, nrows, ncols, blkSize) =>
      MatrixElementDivideExecution(planLater(SumOperator(child, nrows, ncols)), 1, 1,
        planLater(NnzOperator(child, nrows, ncols)), 1, 1, blkSize) :: Nil
    case DiagAvgOperator(child, nrows, ncols, blkSize) =>
      MatrixElementDivideExecution(planLater(TraceOperator(child, nrows, ncols)), 1, 1,
        planLater(DiagNnzOperator(child, nrows, ncols)), 1, 1, blkSize) :: Nil
    case RowMaxOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MatrixTransposeExecution(planLater(ColumnMaxOperator(beforeTrans, ncols, nrows))) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(RowMaxOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        if (alpha >= 0) {
          MatrixScalarMultiplyExecution(planLater(RowMaxOperator(ch, nrows, ncols)), alpha) :: Nil
        } else {
          MatrixScalarMultiplyExecution(planLater(RowMinOperator(ch, nrows, ncols)), alpha) :: Nil
        }
      case _ => RowMaxDirectExecution(planLater(child)) :: Nil
    }
    case ColumnMaxOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MatrixTransposeExecution(planLater(RowMaxOperator(beforeTrans, ncols, nrows))) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(ColumnMaxOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        if (alpha >= 0) {
          MatrixScalarMultiplyExecution(planLater(ColumnMaxOperator(ch, nrows, ncols)),
            alpha) :: Nil
        } else {
          MatrixScalarMultiplyExecution(planLater(ColumnMinOperator(ch, nrows, ncols)),
            alpha) :: Nil
        }
      case _ => ColumnMaxDirectExecution(planLater(child)) :: Nil
    }
    case MaxOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MaxDirectExecution(planLater(beforeTrans)) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(MaxOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        if (alpha >= 0) {
          MatrixScalarMultiplyExecution(planLater(MaxOperator(ch, nrows, ncols)), alpha) :: Nil
        } else {
          MatrixScalarMultiplyExecution(planLater(MinOperator(ch, nrows, ncols)), alpha) :: Nil
        }
      case _ => MaxDirectExecution(planLater(child)) :: Nil
    }
    case DiagMaxOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        DiagMaxDirectExecution(planLater(beforeTrans)) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(DiagMaxOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        if (alpha >= 0) {
          MatrixScalarMultiplyExecution(planLater(DiagMaxOperator(ch, nrows, ncols)), alpha) :: Nil
        } else {
          MatrixScalarMultiplyExecution(planLater(DiagMinOperator(ch, nrows, ncols)), alpha) :: Nil
        }
      case _ => DiagMaxDirectExecution(planLater(child)) :: Nil
    }
    case RowMinOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MatrixTransposeExecution(planLater(ColumnMinOperator(beforeTrans, ncols, nrows))) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(RowMinOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        if (alpha >= 0) {
          MatrixScalarMultiplyExecution(planLater(RowMinOperator(ch, nrows, ncols)), alpha) :: Nil
        } else {
          MatrixScalarMultiplyExecution(planLater(RowMaxOperator(ch, nrows, ncols)), alpha) :: Nil
        }
      case _ => RowMinDirectExecution(planLater(child)) :: Nil
    }
    case ColumnMinOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MatrixTransposeExecution(planLater(RowMinOperator(beforeTrans, ncols, nrows))) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(ColumnMinOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        if (alpha >= 0) {
          MatrixScalarMultiplyExecution(planLater(ColumnMinOperator(ch, nrows, ncols)),
            alpha) :: Nil
        } else {
          MatrixScalarMultiplyExecution(planLater(ColumnMaxOperator(ch, nrows, ncols)),
            alpha) :: Nil
        }
      case _ => ColumnMinDirectExecution(planLater(child)) :: Nil
    }
    case MinOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        MinDirectExecution(planLater(beforeTrans)) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(MinOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        if (alpha >= 0) {
          MatrixScalarMultiplyExecution(planLater(MinOperator(ch, nrows, ncols)), alpha) :: Nil
        } else {
          MatrixScalarMultiplyExecution(planLater(MaxOperator(ch, nrows, ncols)), alpha) :: Nil
        }
      case _ => MinDirectExecution(planLater(child)) :: Nil
    }
    case DiagMinOperator(child, nrows, ncols) => child match {
      case TransposeOperator(beforeTrans) =>
        DiagMinDirectExecution(planLater(beforeTrans)) :: Nil
      case MatrixScalarAddOperator(ch, alpha) =>
        MatrixScalarAddExecution(planLater(DiagMinOperator(ch, nrows, ncols)), alpha) :: Nil
      case MatrixScalarMultiplyOperator(ch, alpha) =>
        if (alpha >= 0) {
          MatrixScalarMultiplyExecution(planLater(DiagMinOperator(ch, nrows, ncols)), alpha) :: Nil
        } else {
          MatrixScalarMultiplyExecution(planLater(DiagMaxOperator(ch, nrows, ncols)), alpha) :: Nil
        }
      case _ => DiagMinDirectExecution(planLater(child)) :: Nil
    }
    case SelectCellValueOperator(child, v, eps) =>
      SelectValueExecution(planLater(child), v, eps) :: Nil
    case MatrixScalarAddOperator(left, right) =>
      MatrixScalarAddExecution(planLater(left), right) :: Nil
    case MatrixScalarMultiplyOperator(left, right) =>
      MatrixScalarMultiplyExecution(planLater(left), right) :: Nil
    case MatrixPowerOperator(left, right) =>
      MatrixPowerExecution(planLater(left), right) :: Nil
    case VectorizeOperator(child, nrows, ncols, blkSize) =>
      VectorizeExecution(planLater(child), nrows, ncols, blkSize) :: Nil
    case MatrixElementAddOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementAddExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case MatrixElementMultiplyOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementMultiplyExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case MatrixElementDivideOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementDivideExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, blkSize) =>
      MatrixMatrixMultiplicationExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case RankOneUpdateOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, blkSize) =>
      RankOneUpdateExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case RemoveEmptyRowsOperator(ch) => RemoveEmptyRowsDirectExecution(planLater(ch)) :: Nil
    case RemoveEmptyColumnsOperator(ch) => RemoveEmptyColumnsDirectExecution(planLater(ch)) :: Nil
    case JoinTwoIndicesOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, mergeFunc, blkSize) =>
      JoinTwoIndicesExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, mergeFunc, blkSize) :: Nil
    case CrossProductOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, mergeFunc, blkSize) =>
      CrossProductExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, mergeFunc, blkSize) :: Nil
    case JoinOnValuesOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, mergeFunc, blkSize) =>
      JoinOnValuesExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, mergeFunc, blkSize) :: Nil
    case JoinIndexValueOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, mode, mergeFunc, blkSize) =>
      JoinIndexValueExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, mode, mergeFunc, blkSize) :: Nil
    case JoinIndexOperator(left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, mode, mergeFunc, blkSize) =>
      JoinIndexExecution(planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, mode, mergeFunc, blkSize) :: Nil
    case GroupBy4DTensorOperator(ch, dims, aggFunc) =>
      GroupBy4DTensorExecution(planLater(ch), dims, aggFunc) :: Nil
    case _ => Nil
  }
}



