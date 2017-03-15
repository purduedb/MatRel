package org.apache.spark.sql.matfast.execution

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, QueryExecution => SQLQueryExecution}
import org.apache.spark.sql.matfast.SparkSession
/**
  * Created by yongyangyu on 11/29/16.
  */
class QueryExecution(val matfastSession: SparkSession, val matfastLogical: LogicalPlan)
  extends SQLQueryExecution(matfastSession, matfastLogical){

  lazy val matrixData: LogicalPlan = {
    assertAnalyzed()
    withCachedData
  }

  override lazy val optimizedPlan: LogicalPlan = {
    matfastSession.sessionState.matfastOptimizer.execute(matfastSession.sessionState.getSQLOptimizer.execute(matrixData))
  }

  override lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(matfastSession)
    matfastSession.sessionState.matfastPlanner.plan(optimizedPlan).next()
  }
}
