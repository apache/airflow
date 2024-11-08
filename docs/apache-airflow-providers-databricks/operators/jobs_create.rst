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



DatabricksCreateJobsOperator
============================

Use the :class:`~airflow.providers.databricks.operators.DatabricksCreateJobsOperator` to create
(or reset) a Databricks job. This operator relies on past XComs to remember the ``job_id`` that
was created so that repeated calls with this operator will update the existing job rather than
creating new ones. When paired with the DatabricksRunNowOperator all runs will fall under the same
job within the Databricks UI.


Using the Operator
------------------

There are three ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the ``api/2.1/jobs/create`` endpoint and pass it directly to our ``DatabricksCreateJobsOperator`` through the
``json`` parameter.  With this approach you get full control over the underlying payload to Jobs REST API, including
execution of Databricks jobs with multiple tasks, but it's harder to detect errors because of the lack of the type checking.

The second way to accomplish the same thing is to use the named parameters of the ``DatabricksCreateJobsOperator`` directly. Note that there is exactly
one named parameter for each top level parameter in the ``api/2.1/jobs/create`` endpoint.

The third way is to use both the json parameter **AND** the named parameters. They will be merged
together. If there are conflicts during the merge, the named parameters will take precedence and
override the top level ``json`` keys.

Currently the named parameters that ``DatabricksCreateJobsOperator`` supports are:
  - ``name``
  - ``description``
  - ``tags``
  - ``tasks``
  - ``job_clusters``
  - ``email_notifications``
  - ``webhook_notifications``
  - ``notification_settings``
  - ``timeout_seconds``
  - ``schedule``
  - ``max_concurrent_runs``
  - ``git_source``
  - ``access_control_list``


Examples
--------

Specifying parameters as JSON
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksCreateJobsOperator is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_jobs_create_json]
    :end-before: [END howto_operator_databricks_jobs_create_json]

Using named parameters
^^^^^^^^^^^^^^^^^^^^^^

You can also use named parameters to initialize the operator and run the job.

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_jobs_create_named]
    :end-before: [END howto_operator_databricks_jobs_create_named]

Pairing with DatabricksRunNowOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can use the ``job_id`` that is returned by the DatabricksCreateJobsOperator in the
return_value XCom as an argument to the DatabricksRunNowOperator to run the job.

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_run_now]
    :end-before: [END howto_operator_databricks_run_now]
