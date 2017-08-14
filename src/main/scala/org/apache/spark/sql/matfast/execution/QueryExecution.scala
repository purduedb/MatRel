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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{QueryExecution => SQLQueryExecution, SparkPlan}
import org.apache.spark.sql.matfast.MatfastSession


class QueryExecution(val matfastSession: MatfastSession, val matfastLogical: LogicalPlan)
  extends SQLQueryExecution(matfastSession, matfastLogical) {

  lazy val matrixData: LogicalPlan = {
    assertAnalyzed()
    withCachedData
  }

  override lazy val optimizedPlan: LogicalPlan = {
    matfastSession.sessionState.matfastOptimizer.execute(
      matfastSession.sessionState.getSQLOptimizer.execute(matrixData))
  }

  override lazy val sparkPlan: SparkPlan = {
    MatfastSession.setActiveSession(matfastSession)
    matfastSession.sessionState.matfastPlanner.plan(optimizedPlan).next()
  }
}
