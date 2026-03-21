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
a new Databricks job via Databricks `api/2.2/jobs/runs/submit <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`_ API endpoint.


Using the Operator
------------------

There are three ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the ``api/2.2/jobs/runs/submit`` endpoint and pass it directly to our ``DatabricksSubmitRunOperator`` through the
``json`` parameter.  With this approach you get full control over the underlying payload to Jobs REST API, including
execution of Databricks jobs with multiple tasks, but it's harder to detect errors because of the lack of the type checking.

.. code-block:: python

  json = {
      "new_cluster": {"spark_version": "2.1.0-db3-scala2.11", "num_workers": 2},
      "notebook_task": {
          "notebook_path": "/Users/airflow@example.com/PrepareData",
      },
  }
  notebook_run = DatabricksSubmitRunOperator(task_id="notebook_run", json=json)

The second way to accomplish the same thing is to use the named parameters of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
one named parameter for each top level parameter in the ``runs/submit`` endpoint.  When using named parameters you must to specify following:

* Task specification - it should be one of:

  * ``spark_jar_task`` - main class and parameters for the JAR task
  * ``notebook_task`` - notebook path and parameters for the task
  * ``spark_python_task`` - python file path and parameters to run the python file with
  * ``spark_submit_task`` - parameters needed to run a ``spark-submit`` command
  * ``pipeline_task`` - parameters needed to run a Delta Live Tables pipeline
  * ``dbt_task`` - parameters needed to run a dbt project

* Cluster specification - it should be one of:
  * ``new_cluster`` - specs for a new cluster on which this task will be run
  * ``existing_cluster_id`` - ID for existing cluster on which to run this task

* ``pipeline_task`` - may refer to either a ``pipeline_id`` or ``pipeline_name``

In the case where both the json parameter **AND** the named parameters
are provided, they will be merged together. If there are conflicts during the merge,
the named parameters will take precedence and override the top level ``json`` keys.

Currently the named parameters that ``DatabricksSubmitRunOperator`` supports are
    - ``spark_jar_task``
    - ``notebook_task``
    - ``spark_python_task``
    - ``spark_submit_task``
    - ``pipeline_task``
    - ``dbt_task``
    - ``git_source``
    - ``new_cluster``
    - ``existing_cluster_id``
    - ``libraries``
    - ``run_name``
    - ``timeout_seconds``

.. code-block:: python

  new_cluster = {"spark_version": "10.1.x-scala2.12", "num_workers": 2}
  notebook_task = {
      "notebook_path": "/Users/airflow@example.com/PrepareData",
  }
  notebook_run = DatabricksSubmitRunOperator(
      task_id="notebook_run", new_cluster=new_cluster, notebook_task=notebook_task
  )

Another way to do is use the param tasks to pass array of objects to instantiate this operator. Here the value of tasks param that is used to invoke ``api/2.2/jobs/runs/submit`` endpoint is passed through the ``tasks`` param in ``DatabricksSubmitRunOperator``. Instead of invoking single task, you can pass array of task and submit a one-time run.

.. code-block:: python

  tasks = [
      {
          "new_cluster": {"spark_version": "2.1.0-db3-scala2.11", "num_workers": 2},
          "notebook_task": {"notebook_path": "/Users/airflow@example.com/PrepareData"},
      }
  ]
  notebook_run = DatabricksSubmitRunOperator(task_id="notebook_run", tasks=tasks)



Examples
--------

Specifying parameters as JSON
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSubmitRunOperator is as follows:

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_json]
    :end-before: [END howto_operator_databricks_json]

Using named parameters
^^^^^^^^^^^^^^^^^^^^^^

You can also use named parameters to initialize the operator and run the job.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_named]
    :end-before: [END howto_operator_databricks_named]

DatabricksSubmitRunDeferrableOperator
=====================================

Deferrable version of the :class:`~airflow.providers.databricks.operators.DatabricksSubmitRunOperator` operator.

It allows to utilize Airflow workers more effectively using `new functionality introduced in Airflow 2.2.0 <https://airflow.apache.org/docs/apache-airflow/2.2.0/concepts/deferring.html#triggering-deferral>`_
