package org.apache.spark.sql.matfast.util


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.matfast.matrix._

/**
  * Created by yongyangyu on 2/21/17.
  */
object MLMatrixSerializer {

  def serialize(obj: MLMatrix): InternalRow = {
    val row = new GenericInternalRow(7)
    obj match {
      case sm: SparseMatrix =>
        row.setByte(0, 0)
        row.setInt(1, sm.numRows)
        row.setInt(2, sm.numCols)
        row.update(3, UnsafeArrayData.fromPrimitiveArray(sm.colPtrs))
        row.update(4, UnsafeArrayData.fromPrimitiveArray(sm.rowIndices))
        row.update(5, UnsafeArrayData.fromPrimitiveArray(sm.values))
        row.setBoolean(6, sm.isTransposed)

      case dm: DenseMatrix =>
        row.setByte(0, 1)
        row.setInt(1, dm.numRows)
        row.setInt(2, dm.numCols)
        row.setNullAt(3)
        row.setNullAt(4)
        row.update(5, UnsafeArrayData.fromPrimitiveArray(dm.values))
        row.setBoolean(6, dm.isTransposed)
    }
    row
  }

  def deserialize(datum: Any): MLMatrix = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 7,
          s"MatrixUDT.deserialize given row with length ${row.numFields} but requires length == 7")
        val tpe = row.getByte(0)
        val numRows = row.getInt(1)
        val numCols = row.getInt(2)
        val values = row.getArray(5).toDoubleArray()
        val isTransposed = row.getBoolean(6)
        tpe match {
          case 0 =>
            val colPtrs = row.getArray(3).toIntArray()
            val rowIndices = row.getArray(4).toIntArray()
            new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
          case 1 =>
            new DenseMatrix(numRows, numCols, values, isTransposed)
        }
    }
  }
}

class MLMatrixSerializer {

}
