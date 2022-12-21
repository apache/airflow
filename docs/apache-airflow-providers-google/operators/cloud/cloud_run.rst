 .. Licensed to the Apache Software Foundation (ASF) under one
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

Google CloudRun Operators
=========================

`Cloud Run <https://cloud.google.com/run/>`__ is Google's fully managed,
Container As A Service solution.
It is a serverless compute platform service which allows users to
deploy applications written in any programming language.

Apart from services, such as web applications exposing http endpoints,
Cloud Run provides creation and execution of jobs running to completion.


Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

.. _howto/operator:CloudRunExecuteJobOperator:

CloudRunExecuteJobOperator
--------------------------

Executes an existing Cloud Run Job.

If ``wait_until_finished`` is set to ``True``,
``CloudRunExecuteJobOperator`` will always wait for job completion.
If set to ``False`` only creates the job execution.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator`.


Reference
---------

For further information, look at:
* `Product Documentation <https://cloud.google.com/run/docs/>`__
