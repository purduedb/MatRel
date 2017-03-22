package org.apache.spark.sql.matfast

import org.apache.spark.sql.matfast.execution.MatfastPlanner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.{execution => sparkexecution, _}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.matfast.plans.{MatfastOptimizer}

import scala.collection.immutable

/**
  * Created by yongyangyu on 2/17/17.
  */

/**
  * A class that holds all session-specific state in a given [[SparkSession]] backed by Simba.
  */
private[matfast] class MatfastSessionState (matfastSession: MatfastSession) extends SessionState(matfastSession){
  self =>

  protected[matfast] lazy val matfastConf = new MatfastConf

  protected[matfast] def getSQLOptimizer = optimizer

  protected[matfast] lazy val matfastOptimizer: MatfastOptimizer = new MatfastOptimizer

  /**
    * Planner that takes into account spatial opt strategies.
    */
  protected[matfast] val matfastPlanner: sparkexecution.SparkPlanner = {
    new MatfastPlanner(matfastSession, conf, experimentalMethods.extraStrategies)
  }

  override def executePlan(plan: LogicalPlan) =
    new execution.QueryExecution(matfastSession, plan)

  def setConf(key: String, value: String): Unit = {
    if (key.startsWith("matfast.")) matfastConf.setConfString(key, value)
    else conf.setConfString(key, value)
  }

  def getConf(key: String): String = {
    if (key.startsWith("matfast.")) matfastConf.getConfString(key)
    else conf.getConfString(key)
  }

  def getConf(key: String, defaultValue: String): String = {
    if (key.startsWith("matfast.")) conf.getConfString(key, defaultValue)
    else conf.getConfString(key, defaultValue)
  }

  def getAllConfs: immutable.Map[String, String] = {
    conf.getAllConfs ++ matfastConf.getAllConfs
  }
}



