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

.. contents::
  :depth: 1
  :local:

Prerequisite
------------

To use ``SparkJDBCOperator`` and ``SparkSubmitOperator``, you must configure a :doc:`Spark Connection <../../connection/spark>`.

.. _howto/operator:SparkJDBCOperator:

SparkJDBCOperator
-----------------

.. _howto/operator:SparkSqlOperator:

SparkSqlOperator
----------------

.. _howto/operator:SparkSubmitOperator:

SparkSubmitOperator
-------------------

Launches applications on a Spark cluster, it uses the ``spark-submit`` script that takes care of setting up the classpath with Spark and its dependencies, and can support different cluster managers and deploy modes that Spark supports.

For parameter definition take a look at :class:`~airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator`.

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/providers/apache/spark/example_dags/example_spark_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spark_submit]
    :end-before: [END howto_operator_spark_submit]

Reference
---------

For further information, look at `Apache Spark submitting applications <https://spark.apache.org/docs/latest/submitting-applications.html>`_.
