package org.apache.spark.sql.matfast.execution

import org.apache.spark.sql.matfast.MatfastSession
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by yongyangyu on 11/29/16.
  */
abstract class MatfastPlan extends SparkPlan{
  @transient
  protected[matfast] final val matfastSessionState = MatfastSession.getActiveSession.map(_.sessionState).orNull

  protected override def sparkContext = MatfastSession.getActiveSession.map(_.sparkContext).orNull
}
