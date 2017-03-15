package org.apache.spark.sql.matfast.execution

import org.apache.spark.sql.matfast.SparkSession
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by yongyangyu on 11/29/16.
  */
abstract class MatfastPlan extends SparkPlan{
  @transient
  protected[matfast] final val matfastSessionState = SparkSession.getActiveSession.map(_.sessionState).orNull

  protected override def sparkContext = SparkSession.getActiveSession.map(_.sparkContext).orNull
}
