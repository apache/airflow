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

Google CloudRun Operator
========================

`Cloud Run <https://cloud.google.com/run/>`__ is Google's fully managed,
Container as a Service (CaaS) Solution.
It is a serverless compute platform service which allows users to
deploy code written in any programming language, with an https endpoint.

Airflow provides operators to make authenticated call against deployed Cloud Run.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: ../_partials/prerequisite_tasks.rst

Cloud Run call
^^^^^^^^^^^^^^

.. _howto/operator:CloudRunOperator:

Send Authenticated HTTP request
"""""""""""""""""""""""""""""""

To send an authenticated HTTP request against a Cloud Run you can use
:class:`~airflow.providers.google.cloud.operators.cloud_run.CloudRunOperator`.

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_run/example_cloud_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloud_run_call]
    :end-before: [END howto_operator_cloud_run_call]

This operator extends `SimpleHttpOperator <https://airflow.apache.org/docs/apache-airflow-providers-http/stable/operators.html>`__.
See the associated doc for extra arguments you can use (header, data).

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/bigquery/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/run/docs/>`__
