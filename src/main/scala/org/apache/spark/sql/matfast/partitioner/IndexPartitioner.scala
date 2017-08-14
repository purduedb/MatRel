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

package org.apache.spark.sql.matfast.partitioner

import org.apache.spark.Partitioner


class IndexPartitioner(partitions: Int) extends Partitioner{

  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions")

  override val numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int) => i
      case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[IndexPartitioner] &&
      numPartitions == other.asInstanceOf[IndexPartitioner].numPartitions
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(partitions: java.lang.Integer)
  }
}
