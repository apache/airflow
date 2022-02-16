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

Communication between Airflow and Looker is done via `Looker API 4.0 <https://docs.looker.com/reference/api-and-integration/api-reference/v4.0>`_.
To facilitate the API communication Looker operators use `Looker SDK <https://pypi.org/project/looker-sdk/>`_ as an API client.
Before calling API, Looker SDK needs to authenticate itself using your Looker API credentials.

* Obtain your Looker API credentials using instructions in the `Looker API documentation <https://docs.looker.com/reference/api-and-integration/api-auth#authentication_with_an_sdk>`_.

* Obtain your Looker API path and port as described in the `Looker API documentation <https://docs.looker.com/reference/api-and-integration/api-getting-started#looker_api_path_and_port>`_.

* Setup a Looker connection in Airflow. You can check :doc:`apache-airflow:howto/connection`

See the following example of a connection setup:

.. code-block::

  Connection Id: your_conn_id  # Passed to operators as ``looker_conn_id`` parameter.
  Connection Type: HTTP
  Host: https://your.looker.com  # Base URL for Looker API. Do not include /api/* in the URL.
  Login: YourClientID  # Looker API client id
  Password: YourClientSecret  # Looker API client secret
  Port: 19999  # Port for Looker API. If hosted on GCP, don't specify the port leaving just the host.
  # Extras are optional parameters in JSON format.
  Extra: {"verify_ssl": "true",  # Set to false only if testing locally against self-signed certs. Defaults to true if not specified.
          "timeout": "120"}  # Timeout in seconds for HTTP requests. Defaults to 2 minutes (120) seconds if not specified.

Start a PDT materialization job
-------------------------------

To submit a PDT materialization job to Looker you need to provide a model and view name.

The job configuration can be submitted in synchronous (blocking) mode by using:
:class:`~airflow.providers.google.cloud.operators.looker.LookerStartPdtBuildOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_looker.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_looker_start_pdt_build_operator]
    :end-before: [END how_to_cloud_looker_start_pdt_build_operator]


Alternatively, the job configuration can be submitted in asynchronous mode by using:
:class:`~airflow.providers.google.cloud.operators.looker.LookerStartPdtBuildOperator` and
:class:`~airflow.providers.google.cloud.sensors.looker.LookerCheckPdtBuildSensor`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_looker.py
    :language: python
    :dedent: 4
    :start-after: [START cloud_looker_async_start_pdt_sensor]
    :end-before: [END cloud_looker_async_start_pdt_sensor]

There are more arguments to provide in the jobs than the examples show.
For the complete list of arguments take a look at Looker operator arguments at
`GitHub <https://github.com/apache/airflow/blob/main/airflow/providers/google/cloud/operators/looker.py>`__
