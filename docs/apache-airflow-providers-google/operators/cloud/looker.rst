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

TODO: [REMOVE] text from here: https://screenshot.googleplex.com/By47uHaoPj2ZMcV (other places have just marketing text)

Looker is a business intelligence software and big data analytics platform that
helps you explore, analyze and share real-time business analytics easily.
Looker has a Public API and associated SDK clients in different languages,
which allow programmatic access to the Looker data platform.

For more information about the service visit `Looker API documentation <https://docs.looker.com/reference/api-and-integration>`_

Prerequisite Tasks
------------------

To use these operators, you must do a few things:

* Obtain Looker API credentials using instructions in the `API documentation <https://docs.looker.com/reference/api-and-integration/api-auth>`_.
* Create a .ini file with the API credentials, as described in the `Configuring the SDK section <https://developers.looker.com/api/getting-started>`_.
* Multiple profiles can be defined in a single .ini file using different sections, as described in the `community article <https://community.looker.com/technical-tips-tricks-1021/the-how-to-on-initializing-the-sdk-with-different-profiles-in-your-ini-file-26846>`_.

See following example on an .ini file:

.. code-block::

    [Looker]
    # Base URL for API. Do not include /api/* in the url
    base_url=https://self-signed.looker.com:19999
    # API 3 client id
    client_id=YourClientID
    # API 3 client secret
    client_secret=YourClientSecret
    # Set to false if testing locally against self-signed certs. Otherwise leave True
    verify_ssl=True

* Install API libraries via **pip**.

.. code-block:: bash

  pip install 'apache-airflow[google]'

Detailed information is available for :doc:`Installation <apache-airflow:installation/index>`.

* Setup a Looker Connection. Path to the .ini file and the optional section in file should be specified in JSON format.

.. note:: If no section is specified, the first section of the .ini file will be used.

See following example of an Airflow Connection setup:

.. code-block::

  Connection Id: "your_conn_id"
  Connection Type: "HTTP"
  Extra: {"config_file": "looker.ini",
          "section": "Looker"}


TODO: ALTERNATIVE WORDING
Before using Looker within Airflow you need to authenticate your account.
Communication between Airflow and Looker is done via Looker SDK.
To get access to Looker API from Looker SDK you need to a .ini file with Looker API credentials.

Please follow the instructions under the 'Configuring the SDK' section
`here <https://developers.looker.com/api/getting-started>`_ to create a .ini file.
Looker API credentials can be obtained by following the instructions
`here <https://docs.looker.com/reference/api-and-integration/api-auth>`_.

Multiple profiles can be defined in a single .ini file using different sections. More on that
`here <https://community.looker.com/technical-tips-tricks-1021/the-how-to-on-initializing-the-sdk-with-different-profiles-in-your-ini-file-26846>`_.

See following example on an .ini file:

.. code-block::

    [Looker]
    # Base URL for API. Do not include /api/* in the url
    base_url=https://self-signed.looker.com:19999
    # API 3 client id
    client_id=YourClientID
    # API 3 client secret
    client_secret=YourClientSecret
    # Set to false if testing locally against self-signed certs. Otherwise leave True
    verify_ssl=True

The path to the .ini file and the optional section in file should be added to the Connection in Airflow in JSON format.
You can check :doc:`apache-airflow:howto/connection`

.. note:: If no section is specified, the first section of the .ini file will be used.

See following example of an Airflow Connection setup:

.. code-block::

  Connection Id: "your_conn_id"
  Connection Type: "HTTP"
  Extra: {"config_file": "looker.ini",
          "section": "Looker"}

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
