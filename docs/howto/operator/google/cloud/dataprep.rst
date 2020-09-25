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

Google Dataprep Operators
=========================
Dataprep is the intelligent cloud data service to visually explore, clean, and prepare data for analysis and machine learning.
Service can be use to explore and transform raw data from disparate and/or large datasets into clean and structured data for further analysis and processing.
Dataprep Job is an internal object encoding the information necessary to run a part of a Cloud Dataprep job group.
For more information about the service visit `Google Dataprep API documentation <https://cloud.google.com/dataprep/docs/html/API-Reference_145281441>`_

Before you begin
^^^^^^^^^^^^^^^^
Before using Dataprep within Airflow you need to authenticate your account with TOKEN.
To get connection Dataprep with Airflow you need Dataprep token. Please follow Dataprep `instructions <https://clouddataprep.com/documentation/api#section/Authentication>`_ to do it.

TOKEN should be added to the Connection in Airflow in JSON format.
You can check `how to do such connection <https://airflow.readthedocs.io/en/stable/howto/connection/index.html#editing-a-connection-with-the-ui>`_.

The DataprepRunJobGroupOperator will run specified job. Operator required a recipe id. To identify the recipe id please use `API documentation for runJobGroup <https://clouddataprep.com/documentation/api#operation/runJobGroup>`_
E.g. if the URL is /flows/10?recipe=7, the recipe id is 7. The recipe cannot be created via this operator. It can be created only via UI which is available `here <https://clouddataprep.com/>`_.
Some of parameters can be override by DAG's body request. How to do it is shown in example dag.

See following example:
Set values for these fields:
.. code-block::

  Conn Id: "your_conn_id"
  Extra: {"extra__dataprep__token": "TOKEN",
          "extra__dataprep__base_url": "https://api.clouddataprep.com"}

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:DataprepRunJobGroupOperator:

Run Job Group
^^^^^^^^^^^^^

Operator task is to create a job group, which launches the specified job as the authenticated user.
This performs the same action as clicking on the Run Job button in the application.

To get information about jobs within a Cloud Dataprep job use:
:class:`~airflow.providers.google.cloud.operators.dataprep.DataprepRunJobGroupOperator`

Example usage:

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_dataprep.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_dataprep_run_job_group_operator]
    :end-before: [END how_to_dataprep_run_job_group_operator]

.. _howto/operator:DataprepGetJobsForJobGroupOperator:

Get Jobs For Job Group
^^^^^^^^^^^^^^^^^^^^^^

Operator task is to get information about the batch jobs within a Cloud Dataprep job.

To get information about jobs within a Cloud Dataprep job use:
:class:`~airflow.providers.google.cloud.operators.dataprep.DataprepGetJobsForJobGroupOperator`

Example usage:

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_dataprep.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_dataprep_get_jobs_for_job_group_operator]
    :end-before: [END how_to_dataprep_get_jobs_for_job_group_operator]

.. _howto/operator:DataprepGetJobGroupOperator:

Get Job Group
^^^^^^^^^^^^^

Operator task is to get the specified job group.
A job group is a job that is executed from a specific node in a flow.

To get information about jobs within a Cloud Dataprep job use:
:class:`~airflow.providers.google.cloud.operators.dataprep.DataprepGetJobGroupOperator`

Example usage:

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_dataprep.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_dataprep_get_job_group_operator]
    :end-before: [END how_to_dataprep_get_job_group_operator]
