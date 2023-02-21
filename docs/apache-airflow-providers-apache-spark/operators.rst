 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Apache Spark Operators
======================

Prerequisite
------------

* To use :class:`~airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator`
  you must configure :doc:`Spark Connection <connections/spark>`.
* To use :class:`~airflow.providers.apache.spark.operators.spark_jdbc.SparkJDBCOperator`
  you must configure both :doc:`Spark Connection <connections/spark>`
  and :doc:`JDBC connection <apache-airflow-providers-jdbc:connections/jdbc>`.
* :class:`~airflow.providers.apache.spark.operators.spark_sql.SparkSqlOperator`
  gets all the configurations from operator parameters.

.. _howto/operator:SparkJDBCOperator:

SparkJDBCOperator
-----------------

Launches applications on a Apache Spark server, it uses ``SparkSubmitOperator`` to perform data transfers to/from JDBC-based databases.

For parameter definition take a look at :class:`~airflow.providers.apache.spark.operators.spark_jdbc.SparkJDBCOperator`.

Using the operator
""""""""""""""""""

Using ``cmd_type`` parameter, is possible to transfer data from Spark to a database (``spark_to_jdbc``) or from a database to Spark (``jdbc_to_spark``), which will write the table using the Spark command ``saveAsTable``.

.. exampleinclude:: /../../tests/system/providers/apache/spark/example_spark_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spark_jdbc]
    :end-before: [END howto_operator_spark_jdbc]


Reference
"""""""""

For further information, look at `Apache Spark DataFrameWriter documentation <https://spark.apache.org/docs/2.4.5/api/scala/index.html#org.apache.spark.sql.DataFrameWriter>`_.

.. _howto/operator:SparkSqlOperator:

SparkSqlOperator
----------------

Launches applications on a Apache Spark server, it requires that the ``spark-sql`` script is in the PATH.
The operator will run the SQL query on Spark Hive metastore service, the ``sql`` parameter can be templated and be a ``.sql`` or ``.hql`` file.

For parameter definition take a look at :class:`~airflow.providers.apache.spark.operators.spark_sql.SparkSqlOperator`.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../tests/system/providers/apache/spark/example_spark_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spark_sql]
    :end-before: [END howto_operator_spark_sql]

Reference
"""""""""

For further information, look at `Running the Spark SQL CLI <https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html#running-the-spark-sql-cli>`_.

.. _howto/operator:SparkSubmitOperator:

SparkSubmitOperator
-------------------

Launches applications on a Apache Spark server, it uses the ``spark-submit`` script that takes care of setting up the classpath with Spark and its dependencies, and can support different cluster managers and deploy modes that Spark supports.

For parameter definition take a look at :class:`~airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator`.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../../tests/system/providers/apache/spark/example_spark_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spark_submit]
    :end-before: [END howto_operator_spark_submit]

Reference
"""""""""

For further information, look at `Apache Spark submitting applications <https://spark.apache.org/docs/latest/submitting-applications.html>`_.
