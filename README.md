New branch of MatFast project. 
The code base moves from Spark-1.5.0 to Spark-2.1.
We have changed the project name to "MatFast". Furthermore, 
this project used to be a standalone application on Spark-1.5.0. 
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

For the dev branch, we are actively extending various operators on top of 
matrix data. Specifically, we are supporting relational operators 
over matrix data, i.e., relational selection, projection, aggregation, 
and join operations. We'll update the semantics of these operators and 
how they inter-operate with existing matrix operators in a separate 
technique report.

