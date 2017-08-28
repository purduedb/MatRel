[![Build Status](https://travis-ci.org/merlintang/SparkDistributedMatrix.svg?branch=branch-2.1)](https://travis-ci.org/merlintang/SparkDistributedMatrix)

# Apache Spark&trade; Distributed Matrix Computation

A library to support distributed matrix computation for machine learning and data analysis  


## Build and run matFast

matFast is built using [Apache Maven](http://maven.apache.org/).
To build matFast and its example programs, run:

    mvn clean package -DskipTests

by default this project will be built against spark-2.1.0 with scala-2.11,
if you want to specify other version, use maven `-D` parameter such as:

    mvn clean package -Dscala.binary.version=2.10 -Dspark.version=2.1.0

matFast also support SBT based.
To build matFast and its example programs, run:

    sbt package

    sbt assembly

## Reference

Now, We are integrating the matrix operations and optimizations 
inside Spark SQL. We have extended the DataSet API such that 
all the implemented matrix operators are available to Spark SQL. 
We are also working on extending the Spark SQL Catalyst such 
that the optimizer can automatically recognize the special 
structures of matrix queries.

This work was accepted by ICDE'17. More technical details and 
design decisions can be found in the paper "In-memory distributed matrix 
computation processing and optimization". We'll continue updating 
the code base to embrace more useful features.

## Contact & Acknowledgements

