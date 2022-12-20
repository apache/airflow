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

.. _howto/operator:SASJobExecutionOperator:

SASJobExecutionOperator
=======================
airflow.providers.sas.operators.sas_jobexecution
The purpose of SAS Job Execution Operator is to allow executing a SAS job.

Use the :class:`~airflow.providers.sas.operators.sas_jobexecution` to execute SAS jobs.

Common Usage
------------

To use the job execution operator, two parameters are required: ``connection_name`` and ``job_name``. You must create an HTTP connection in Airflow, either via the UI or through programmatic means. The connection should have a host name specified which should be in the form ``https://<hostname>[:port]``. You can specify a logon and password as well. If you'd rather specify an OAuth token you can leave the logon and password empty and specify the OAuth token in the extra field. It should be in json format with one element called token for example: ``{"token":"token_here"}``.
The ``job_name`` parameter should reflect a path to the job, for example ``/Public/AirflowJobs/jobName``. There is an optional parameter ``parameters`` of type dict which can be used to pass values into the program. These are added before the program is run.

Example
-------

The code snippets below are based on Airflow-2.6

An example usage of the SASJobExecutionOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/sas/example_sas_jobexecution.py
    :language: python
    :start-after: [START sas_jes_howto_operator]
    :end-before: [END sas_jes_howto_operator]


The complete Job Execution Operator DAG
---------------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/sas/example_sas_jobexecution.py
    :language: python
    :start-after: [START sas_jes_howto]
    :end-before: [END sas_jes_howto]
