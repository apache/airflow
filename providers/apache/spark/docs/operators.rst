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
  you must configure :doc:`Spark Connection <connections/spark-submit>`.
* To use :class:`~airflow.providers.apache.spark.operators.spark_jdbc.SparkJDBCOperator`
  you must configure both :doc:`Spark Connection <connections/spark-submit>`
  and :doc:`JDBC connection <apache-airflow-providers-jdbc:connections/jdbc>`.
* :class:`~airflow.providers.apache.spark.operators.spark_sql.SparkSqlOperator`
  gets all the configurations from operator parameters.
* To use :class:`~airflow.providers.apache.spark.operators.spark_pyspark.PySparkOperator`
  you can configure :doc:`SparkConnect Connection <connections/spark-connect>`.
* To use :class:`~airflow.providers.apache.spark.operators.spark_pipelines.SparkPipelinesOperator`
  you must configure :doc:`Spark Connection <connections/spark-submit>` and have the ``spark-pipelines`` CLI available.

.. _howto/operator:SparkJDBCOperator:

SparkJDBCOperator
-----------------

Launches applications on a Apache Spark server, it uses ``SparkSubmitOperator`` to perform data transfers to/from JDBC-based databases.

For parameter definition take a look at :class:`~airflow.providers.apache.spark.operators.spark_jdbc.SparkJDBCOperator`.

Using the operator
""""""""""""""""""

Using ``cmd_type`` parameter, is possible to transfer data from Spark to a database (``spark_to_jdbc``) or from a database to Spark (``jdbc_to_spark``), which will write the table using the Spark command ``saveAsTable``.

.. exampleinclude:: /../tests/system/apache/spark/example_spark_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spark_jdbc]
    :end-before: [END howto_operator_spark_jdbc]


Reference
"""""""""

For further information, look at `Apache Spark DataFrameWriter documentation <https://spark.apache.org/docs/2.4.5/api/scala/index.html#org.apache.spark.sql.DataFrameWriter>`_.

.. _howto/operator:PySparkOperator:

PySparkOperator
----------------

Launches applications on a Apache Spark Connect server or directly in a standalone mode

For parameter definition take a look at :class:`~airflow.providers.apache.spark.operators.spark_pyspark.PySparkOperator`.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../tests/system/apache/spark/example_spark_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spark_pyspark]
    :end-before: [END howto_operator_spark_pyspark]

Reference
"""""""""

For further information, look at `Running the Spark Connect Python <https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_connect.html>`_.

.. _howto/operator:SparkPipelinesOperator:

SparkPipelinesOperator
----------------------

Execute Spark Declarative Pipelines using the ``spark-pipelines`` CLI. This operator wraps the spark-pipelines binary to execute declarative data pipelines, supporting both pipeline execution and validation through dry-runs.

For parameter definition take a look at :class:`~airflow.providers.apache.spark.operators.spark_pipelines.SparkPipelinesOperator`.

Using the operator
""""""""""""""""""

The operator can be used to run declarative pipelines:

.. code-block:: python

   from airflow.providers.apache.spark.operators.spark_pipelines import SparkPipelinesOperator

   # Execute the pipeline
   run_pipeline = SparkPipelinesOperator(
       task_id="run_pipeline",
       pipeline_spec="/path/to/pipeline.yml",
       pipeline_command="run",
       conn_id="spark_default",
       num_executors=2,
       executor_cores=4,
       executor_memory="2G",
       driver_memory="1G",
   )

**Pipeline Specification**

The ``pipeline_spec`` parameter should point to a YAML file defining your declarative pipeline:

.. code-block:: yaml

   name: my_pipeline
   storage: file:///path/to/pipeline-storage
   libraries:
     - glob:
         include: transformations/**

**Pipeline Commands**

* ``run`` - Execute the pipeline (default)
* ``dry-run`` - Validate the pipeline without execution

Reference
"""""""""

For further information, look at `Spark Declarative Pipelines Programming Guide <https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html>`_.

.. _howto/operator:SparkSqlOperator:

SparkSqlOperator
----------------

Launches applications on a Apache Spark server, it requires that the ``spark-sql`` script is in the PATH.
The operator will run the SQL query on Spark Hive metastore service, the ``sql`` parameter can be templated and be a ``.sql`` or ``.hql`` file.

For parameter definition take a look at :class:`~airflow.providers.apache.spark.operators.spark_sql.SparkSqlOperator`.

Using the operator
""""""""""""""""""

.. exampleinclude:: /../tests/system/apache/spark/example_spark_dag.py
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

.. exampleinclude:: /../tests/system/apache/spark/example_spark_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spark_submit]
    :end-before: [END howto_operator_spark_submit]

Reference
"""""""""

For further information, look at `Apache Spark submitting applications <https://spark.apache.org/docs/latest/submitting-applications.html>`_.

Cluster mode crash recovery (Spark standalone)
"""""""""""""""""""""""""""""""""""""""""""""""

When running in Spark standalone cluster mode (``--deploy-mode cluster``), the Spark driver runs
independently on the cluster. If the Airflow worker dies while the Spark job is running, the driver keeps running but
Airflow loses track of it and the behaviour to submit a brand new job would be wasting
the compute already done.

Now, the ``SparkSubmitOperator`` solves this by persisting the driver ID to ``task_state`` immediately after
submission. On retry, it reads the ID back and reconnects to the already-running driver instead of
resubmitting.

This is the **synchronous path** — the worker holds a slot for the duration of polling. This is
a crash-safety net for teams running sync operators for log observability, org constraints, or
because a Triggerer is not available. Teams with a Triggerer available may also consider
deferrable operators, which free the worker slot but may come with added complexity.

.. note::
    Crash recovery in cluster mode requires Airflow 3.3+ (``task_state`` support). On earlier
    versions the operator falls back to the previous behavior of always submitting fresh.
