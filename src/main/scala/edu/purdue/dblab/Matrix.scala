package edu.purdue.dblab

/**
 * Created by yongyangyu on 6/25/15.
 */
trait Matrix extends Serializable{
    /*
     *  nRows() returns the number of rows
     */
    def nRows(): Long

    /*
     *  nCols() returns the number of columns
     */
    def nCols(): Long

    /*
     *  nnz() returns the number of nonzero elements,
     *  especially useful for sparse matrices
     */
    def nnz(): Long
}
