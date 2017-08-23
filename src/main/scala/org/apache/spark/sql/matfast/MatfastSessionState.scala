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

import org.apache.spark.sql.matfast.execution.MatfastPlanner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.{execution => sparkexecution, _}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.matfast.plans.{MatfastOptimizer}

import scala.collection.immutable


private[matfast] class MatfastSessionState (matfastSession: MatfastSession)
  extends SessionState(matfastSession) {
  self =>

  protected[matfast] lazy val matfastConf = new MatfastConf

  protected[matfast] def getSQLOptimizer = optimizer

  protected[matfast] lazy val matfastOptimizer: MatfastOptimizer = new MatfastOptimizer

  /**
    * Planner that takes into account matrix opt strategies.
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



