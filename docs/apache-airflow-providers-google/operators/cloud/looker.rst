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

Google Cloud Looker Operators
===============================

Looker is a business intelligence software and big data analytics platform that
helps you explore, analyze and share real-time business analytics easily.

Looker has a Public API and associated SDK clients in different languages,
which allow programmatic access to the Looker data platform.

For more information visit `Looker API documentation <https://docs.looker.com/reference/api-and-integration>`_.

Prerequisite Tasks
------------------

To use these operators, you must do a few things:

* Install API libraries via **pip**.

.. code-block:: bash

  pip install 'apache-airflow[google]'

Detailed information is available for :doc:`Installation <apache-airflow:installation/index>`.

* Setup a Looker connection in Airflow. You can check :doc:`apache-airflow:howto/connection` and :doc:`/connections/gcp_looker`

Start a PDT materialization job
-------------------------------

To submit a PDT materialization job to Looker you need to provide a model and view name.

The job configuration can be submitted in synchronous (blocking) mode by using:
:class:`~airflow.providers.google.cloud.operators.looker.LookerStartPdtBuildOperator`.

.. exampleinclude:: /../../providers/src/airflow/providers/google/cloud/example_dags/example_looker.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_looker_start_pdt_build_operator]
    :end-before: [END how_to_cloud_looker_start_pdt_build_operator]


Alternatively, the job configuration can be submitted in asynchronous mode by using:
:class:`~airflow.providers.google.cloud.operators.looker.LookerStartPdtBuildOperator` and
:class:`~airflow.providers.google.cloud.sensors.looker.LookerCheckPdtBuildSensor`.

.. exampleinclude:: /../../providers/src/airflow/providers/google/cloud/example_dags/example_looker.py
    :language: python
    :dedent: 4
    :start-after: [START cloud_looker_async_start_pdt_sensor]
    :end-before: [END cloud_looker_async_start_pdt_sensor]

There are more arguments to provide in the jobs than the examples show.
For the complete list of arguments take a look at Looker operator arguments at :class:`airflow.providers.google.cloud.operators.looker.LookerStartPdtBuildOperator`
