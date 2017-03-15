package org.apache.spark.sql.matfast.plans

import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughJoin
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}

/**
  * Created by yongyangyu on 2/20/17.
  */
class MatfastOptimizer extends RuleExecutor[LogicalPlan]{
  val batches =
    Batch("LocalRelation", FixedPoint(100), PushPredicateThroughJoin) :: Nil
}
