..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Experimental Rest API
=====================

Airflow exposes an experimental Rest API. It is available through the webserver. Endpoints are
available at /api/experimental/. Please note that we expect the endpoint definitions to change.

Endpoints
---------

.. http:post:: /api/experimental/dags/<DAG_ID>/dag_runs


  Creates a dag_run for a given dag id.


  **Trigger DAG with config, example:**

  .. code-block:: bash

    curl -X POST \
      http://localhost:8080/api/experimental/dags/<DAG_ID>/dag_runs \
      -H 'Cache-Control: no-cache' \
      -H 'Content-Type: application/json' \
      -d '{"conf":"{\"key\":\"value\"}"}'


.. http:get:: /api/experimental/dags/<DAG_ID>/dag_runs

  Returns a list of Dag Runs for a specific DAG ID.

.. http:get:: /api/experimental/dags/<string:dag_id>/dag_runs/<string:execution_date>

  Returns a JSON with a dag_run's public instance variables. The format for the <string:execution_date> is expected to be "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".


.. http:get:: /api/experimental/test

  To check REST API server correct work. Return status 'OK'.


.. http:get:: /api/experimental/dags/<DAG_ID>/tasks/<TASK_ID>

  Returns info for a task.


.. http:get:: /api/experimental/dags/<DAG_ID>/dag_runs/<string:execution_date>/tasks/<TASK_ID>

  Returns a JSON with a task instance's public instance variables. The format for the <string:execution_date> is expected to be "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".


.. http:get:: /api/experimental/dags/<DAG_ID>/paused/<string:paused>

  '<string:paused>' must be a 'true' to pause a DAG and 'false' to unpause.


.. http:get:: /api/experimental/latest_runs

  Returns the latest DagRun for each DAG formatted for the UI.


.. http:get:: /api/experimental/pools

  Get all pools.


.. http:get:: /api/experimental/pools/<string:name>

  Get pool by a given name.

.. http:post:: /api/experimental/pools

  Create a pool.

.. http:delete:: /api/experimental/pools/<string:name>

  Delete pool.


CLI
-----

For some functions the cli can use the API. To configure the CLI to use the API when available
configure as follows:

.. code-block:: bash

    [cli]
    api_client = airflow.api.client.json_client
    endpoint_url = http://<WEBSERVER>:<PORT>


Authentication
--------------

Authentication for the API is handled separately to the Web Authentication. The default is to not
require any authentication on the API -- i.e. wide open by default. This is not recommended if your
Airflow webserver is publicly accessible, and you should probably use the deny all backend:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.deny_all

Two "real" methods for authentication are currently supported for the API.

To enabled Password authentication, set the following in the configuration:

.. code-block:: bash

    [api]
    auth_backend = airflow.contrib.auth.backends.password_auth

It's usage is similar to the Password Authentication used for the Web interface.

To enable Kerberos authentication, set the following in the configuration:

.. code-block:: ini

    [api]
    auth_backend = airflow.api.auth.backend.kerberos_auth

    [kerberos]
    keytab = <KEYTAB>

The Kerberos service is configured as ``airflow/fully.qualified.domainname@REALM``. Make sure this
principal exists in the keytab file.
