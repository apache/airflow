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
`Google Dataprep API documentation is available here <https://cloud.google.com/dataprep/docs/html/API-Reference_145281441>`__

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:DataprepGetJobsForJobGroupOperator:

Get Jobs For Job Group
^^^^^^^^^^^^^^^^^^^^^^

To get information about jobs within a Cloud Dataprep job use:
:class:`~airflow.providers.google.cloud.operators.dataprep.DataprepGetJobsForJobGroupOperator`

To get connection Dataprep with Airflow you need Dataprep token.
Please follow Dataprep instructions.
https://clouddataprep.com/documentation/api#section/Authentication

It should be added to the Connection in Airflow in JSON format.
Her you can check how to do such connection:
https://airflow.readthedocs.io/en/stable/howto/connection/index.html#editing-a-connection-with-the-ui

See following example:
Set values for these fields:
.. code-block::

  Conn Id: "your_conn_id"
  Extra: "{\"token\": \"TOKEN\"}

Example usage:

.. exampleinclude:: /../airflow/providers/google/cloud/example_dags/example_dataprep.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_dataprep_get_jobs_for_job_group_operator]
    :end-before: [END how_to_dataprep_get_jobs_for_job_group_operator]
