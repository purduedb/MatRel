package org.apache.spark.sql.matfast.execution

import org.apache.spark.sql.matfast.SparkSession
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
class MatfastPlanner(val matfastContext: SparkSession,
                     override val conf: SQLConf,
                     override val extraStrategies: Seq[Strategy])
  extends SparkPlanner(matfastContext.sparkContext, conf, extraStrategies) {

  override def strategies: Seq[Strategy] =
    (MatrixOperators :: Nil) ++ super.strategies
}

object MatrixOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case TranposeOperator(child) => MatrixTransposeExecution(planLater(child)) :: Nil
    case MatrixScalarAddOperator(left, right) =>
      MatrixScalarAddExecution(planLater(left), right) :: Nil
    case MatrixScalarMultiplyOperator(left, right) =>
      MatrixScalarMultiplyExecution(planLater(left), right) :: Nil
    case MatrixPowerOperator(left, right) =>
      MatrixPowerExecution(planLater(left), right) :: Nil
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



