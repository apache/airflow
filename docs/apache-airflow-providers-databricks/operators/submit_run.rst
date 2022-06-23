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



.. _howto/operator:DatabricksSubmitRunOperator:


DatabricksSubmitRunOperator
===========================

Use the :class:`~airflow.providers.databricks.operators.DatabricksSubmitRunOperator` to submit
a new Databricks job via Databricks `api/2.1/jobs/runs/submit <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`_ API endpoint.


Using the Operator
------------------

There are two ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the ``api/2.1/jobs/runs/submit`` endpoint and pass it directly to our ``DatabricksSubmitRunOperator`` through the
``json`` parameter.  With this approach you get full control over the underlying payload to Jobs REST API, including
execution of Databricks jobs with multiple tasks, but it's harder to detect errors because of the lack of the type checking.

Another way to accomplish the same thing is to use the named parameters of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
one named parameter for each top level parameter in the ``runs/submit`` endpoint.  When using named parameters you must to specify following:

* Task specification - it should be one of:

  * ``spark_jar_task`` - main class and parameters for the JAR task
  * ``notebook_task`` - notebook path and parameters for the task
  * ``spark_python_task`` - python file path and parameters to run the python file with
  * ``spark_submit_task`` - parameters needed to run a ``spark-submit`` command
  * ``pipeline_task`` - parameters needed to run a Delta Live Tables pipeline

* Cluster specification - it should be one of:
  * ``new_cluster`` - specs for a new cluster on which this task will be run
  * ``existing_cluster_id`` - ID for existing cluster on which to run this task

All other parameters are optional, and described in the documentation of the ``DatabricksSubmitRunOperator`` class.


Examples
--------

Specifying parameters as JSON
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSubmitRunOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_json]
    :end-before: [END howto_operator_databricks_json]

Using named parameters
^^^^^^^^^^^^^^^^^^^^^^

You can also use named parameters to initialize the operator and run the job.

.. exampleinclude:: /../../tests/system/providers/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_named]
    :end-before: [END howto_operator_databricks_named]

DatabricksSubmitRunDeferrableOperator
=====================================

Deferrable version of the :class:`~airflow.providers.databricks.operators.DatabricksSubmitRunOperator` operator.

It allows to utilize Airflow workers more effectively using `new functionality introduced in Airflow 2.2.0 <https://airflow.apache.org/docs/apache-airflow/2.2.0/concepts/deferring.html#triggering-deferral>`_
