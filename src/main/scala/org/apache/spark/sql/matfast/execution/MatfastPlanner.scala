package org.apache.spark.sql.matfast.execution

import org.apache.spark.sql.matfast.MatfastSession
import org.apache.spark.sql.matfast.execution._
import org.apache.spark.sql.matfast.plans._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Strategy, catalyst}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner}
import org.apache.spark.sql.internal.SQLConf

/**
  * Created by yongyangyu on 11/29/16.
  */
class MatfastPlanner(val matfastContext: MatfastSession,
                     override val conf: SQLConf,
                     override val extraStrategies: Seq[Strategy])
  extends SparkPlanner(matfastContext.sparkContext, conf, extraStrategies) {

  override def strategies: Seq[Strategy] =
    (MatrixOperators :: Nil) ++ super.strategies
}

object MatrixOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case TranposeOperator(child) => MatrixTransposeExecution(planLater(child)) :: Nil
    case RowSumOperator(child, nrows, ncols) => child match {
      case TranposeOperator(beforeTrans) =>
        MatrixTransposeExecution(ColumnSumDirectExecution(planLater(beforeTrans))) :: Nil
      case MatrixScalarAddOperator(child, alpha) =>
        MatrixScalarAddExecution(RowSumDirectExecution(planLater(child)), alpha*ncols) :: Nil
      case MatrixScalarMultiplyOperator(child, alpha) =>
        MatrixScalarMultiplyExecution(RowSumDirectExecution(planLater(child)), alpha) :: Nil
      case MatrixElementAddOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
        MatrixElementAddExecution(RowSumDirectExecution(planLater(left)), leftRowNum, 1L,
          RowSumDirectExecution(planLater(right)), rightRowNum, 1L, blkSize) :: Nil
      case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
        MatrixMatrixMultiplicationExecution(planLater(left), leftRowNum, leftColNum,
          RowSumDirectExecution(planLater(right)), rightRowNum, 1L, blkSize) :: Nil
      case _ => RowSumDirectExecution(planLater(child)) :: Nil // default on an given matrix input
    }
    case ColumnSumOperator(child, nrows, ncols) => child match {
      case TranposeOperator(beforeTrans) =>
        MatrixTransposeExecution(RowSumDirectExecution(planLater(beforeTrans))) :: Nil
      case MatrixScalarAddOperator(child, alpha) =>
        MatrixScalarAddExecution(ColumnSumDirectExecution(planLater(child)), alpha*nrows) :: Nil
      case MatrixScalarMultiplyOperator(child, alpha) =>
        MatrixScalarMultiplyExecution(ColumnSumDirectExecution(planLater(child)), alpha) :: Nil
      case MatrixElementAddOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
        MatrixElementAddExecution(ColumnSumDirectExecution(planLater(left)), 1L, leftColNum,
          ColumnSumDirectExecution(planLater(right)), 1L, rightColNum, blkSize) :: Nil
      case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
        MatrixMatrixMultiplicationExecution(ColumnSumDirectExecution(planLater(left)), 1L, leftColNum,
          planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
      case _ => ColumnSumDirectExecution(planLater(child)) :: Nil
    }
    case SumOperator(child, nrows, ncols) => child match {
      case TranposeOperator(beforeTans) => SumDirectExecution(planLater(beforeTans)) :: Nil
      case MatrixScalarAddOperator(child, alpha) =>
        MatrixScalarAddExecution(SumDirectExecution(planLater(child)), alpha*nrows*ncols) :: Nil
      case MatrixScalarMultiplyOperator(child, alpha) =>
        MatrixScalarMultiplyExecution(SumDirectExecution(planLater(child)), alpha) :: Nil
      case MatrixElementAddOperator(left, leftRowNum, leftColNum, right, rightRow, rightColNum, blkSize) =>
        MatrixElementAddExecution(SumDirectExecution(planLater(left)), 1L, 1L, SumDirectExecution(planLater(right)), 1L, 1L, blkSize) :: Nil
      case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
        MatrixMatrixMultiplicationExecution(ColumnSumDirectExecution(planLater(left)), 1L, leftColNum,
          RowSumDirectExecution(planLater(right)), rightRowNum, 1L, blkSize) :: Nil
      case _ => SumDirectExecution(planLater(child)) :: Nil
    }
    case MatrixScalarAddOperator(left, right) =>
      MatrixScalarAddExecution(planLater(left), right) :: Nil
    case MatrixScalarMultiplyOperator(left, right) =>
      MatrixScalarMultiplyExecution(planLater(left), right) :: Nil
    case MatrixPowerOperator(left, right) =>
      MatrixPowerExecution(planLater(left), right) :: Nil
    case VectorizeOperator(child, nrows, ncols, blkSize) =>
      VectorizeExecution(planLater(child), nrows, ncols, blkSize) :: Nil
    case MatrixElementAddOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementAddExecution(planLater(left), leftRowNum, leftColNum, planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case MatrixElementMultiplyOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementMultiplyExecution(planLater(left), leftRowNum, leftColNum, planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case MatrixElementDivideOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementDivideExecution(planLater(left), leftRowNum, leftColNum, planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case MatrixMatrixMultiplicationOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
      MatrixMatrixMultiplicationExecution(planLater(left), leftRowNum, leftColNum, planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case RankOneUpdateOperator(left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
      RankOneUpdateExecution(planLater(left), leftRowNum, leftColNum, planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
    case _ => Nil
  }
}



