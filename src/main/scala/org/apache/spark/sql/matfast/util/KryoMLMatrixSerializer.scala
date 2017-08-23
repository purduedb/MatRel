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

package org.apache.spark.sql.matfast.util

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.sql.matfast.matrix._

class KryoMLMatrixSerializer extends Serializer[MLMatrix]{

  private def getTypeInt(m: MLMatrix): Short = m match {
    case _: SparseMatrix => 0
    case _: DenseMatrix => 1
    case _ => -1
  }

   override def write(kryo: Kryo, output: Output, matrix: MLMatrix) {
    output.writeShort(getTypeInt(matrix))
    matrix match {
      case dense: DenseMatrix =>
        output.writeInt(dense.numRows, true)
        output.writeInt(dense.numCols, true)
        output.writeInt(dense.values.length, true)
        dense.values.foreach(output.writeDouble)
        output.writeBoolean(dense.isTransposed)
      case sp: SparseMatrix =>
        output.writeInt(sp.numRows, true)
        output.writeInt(sp.numCols, true)
        output.writeInt(sp.colPtrs.length, true)
        sp.colPtrs.foreach(x => output.writeInt(x, true))
        output.writeInt(sp.rowIndices.length, true)
        sp.rowIndices.foreach(x => output.writeInt(x, true))
        output.writeInt(sp.values.length, true)
        sp.values.foreach(output.writeDouble)
        output.writeBoolean(sp.isTransposed)
    }
  }

  override def read(kryo: Kryo, input: Input, typ: Class[MLMatrix]): MLMatrix = {
    val typInt = input.readShort()
    if (typInt == 1) { // DenseMatrix
      val numRows = input.readInt(true)
      val numCols = input.readInt(true)
      val dim = input.readInt(true)
      val values = Array.ofDim[Double](dim)
      for (i <- 0 until dim) values(i) = input.readDouble()
      val isTransposed = input.readBoolean()
      new DenseMatrix(numRows, numCols, values, isTransposed)
    } else if (typInt == 0) { // SparseMatrix
      val numRows = input.readInt(true)
      val numCols = input.readInt(true)
      val colPtrsDim = input.readInt(true)
      val colPtrs = Array.ofDim[Int](colPtrsDim)
      for (i <- 0 until colPtrsDim) colPtrs(i) = input.readInt(true)
      val rowIndicesDim = input.readInt(true)
      val rowIndices = Array.ofDim[Int](rowIndicesDim)
      for (i <- 0 until rowIndicesDim) rowIndices(i) = input.readInt(true)
      val valueDim = input.readInt(true)
      val values = Array.ofDim[Double](valueDim)
      for (i <- 0 until valueDim) values(i) = input.readDouble()
      val isTransposed = input.readBoolean()
      new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
    } else null
  }
}
