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
    case SelectOperator(child, nrows, ncols, blkSize, rowIdx, colIdx) =>
      child match {
        case TransposeOperator(ch) =>
          SelectDirectExecution(planLater(ch), blkSize, colIdx, rowIdx) :: Nil
        case MatrixScalarAddOperator(ch, alpha) =>
          MatrixScalarAddExecution(planLater(
            SelectOperator(ch, nrows, ncols, blkSize, rowIdx, colIdx)), alpha) :: Nil
        case MatrixScalarMultiplyOperator(ch, alpha) =>
          MatrixScalarMultiplyExecution(planLater(
            SelectOperator(ch, nrows, ncols, blkSize, rowIdx, colIdx)), alpha) :: Nil
        case MatrixElementAddOperator(left, leftRowNum, leftColNum,
        right, rightRowNum, rightColNum, blkSize) =>
          MatrixElementAddExecution(planLater(
            SelectOperator(left, leftRowNum, leftColNum, blkSize, rowIdx, colIdx)),
            leftRowNum, leftColNum, planLater(
            SelectOperator(right, rightRowNum, rightColNum, blkSize, rowIdx, colIdx)),
            rightRowNum, rightColNum, blkSize) :: Nil
        case MatrixElementMultiplyOperator(left, leftRowNum, leftColNum,
        right, rightRowNum, rightColNum, blkSize) =>
          MatrixElementMultiplyExecution(planLater(
            SelectOperator(left, leftRowNum, leftColNum, blkSize, rowIdx, colIdx)),
            leftRowNum, leftColNum, planLater(
            SelectOperator(right, rightRowNum, rightColNum, blkSize, rowIdx, colIdx)),
            rightRowNum, rightColNum, blkSize) :: Nil
        case MatrixElementDivideOperator(left, leftRowNum, leftColNum,
        right, rightRowNum, rightColNum, blkSize) =>
          MatrixElementDivideExecution(planLater(
            SelectOperator(left, leftRowNum, leftColNum, blkSize, rowIdx, colIdx)),
            leftRowNum, leftColNum, planLater(
            SelectOperator(right, rightRowNum, rightColNum, blkSize, rowIdx, colIdx)),
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

    case _ => Nil
  }
}



