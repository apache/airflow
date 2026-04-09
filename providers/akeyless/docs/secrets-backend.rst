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

Akeyless Secrets Backend
========================

Use Akeyless as the secrets backend for Apache Airflow to source Connections,
Variables, and Configuration options directly from the Akeyless Vault Platform.

Configuration
-------------

Add to ``airflow.cfg``:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.akeyless.secrets.akeyless.AkeylessBackend
    backend_kwargs = {
        "connections_path": "/airflow/connections",
        "variables_path": "/airflow/variables",
        "config_path": "/airflow/config",
        "api_url": "https://api.akeyless.io",
        "access_id": "p-xxxxxxxxx",
        "access_key": "your-access-key",
        "access_type": "api_key"
    }

Or via environment variable:

.. code-block:: bash

    export AIRFLOW__SECRETS__BACKEND="airflow.providers.akeyless.secrets.akeyless.AkeylessBackend"
    export AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_path": "/airflow/connections", ...}'

Secret Naming Convention
------------------------

Secrets are resolved by joining ``<base_path>/<key>``:

.. list-table::
   :header-rows: 1

   * - Type
     - Example lookup path
   * - Connection ``postgres_default``
     - ``/airflow/connections/postgres_default``
   * - Variable ``my_var``
     - ``/airflow/variables/my_var``
   * - Config ``smtp_host``
     - ``/airflow/config/smtp_host``

Storing Connections
-------------------

Connections can be stored in three formats:

**URI string:**

.. code-block:: text

    postgresql://user:password@host:5432/dbname

**JSON dict with ``conn_uri``:**

.. code-block:: json

    {"conn_uri": "postgresql://user:password@host:5432/dbname"}

**JSON dict with individual fields:**

.. code-block:: json

    {
        "conn_type": "postgres",
        "host": "db.example.com",
        "login": "admin",
        "password": "secret",
        "schema": "mydb",
        "port": 5432
    }

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Parameter
     - Default
     - Description
   * - ``connections_path``
     - ``/airflow/connections``
     - Akeyless folder path for connections. Set to *None* to disable.
   * - ``variables_path``
     - ``/airflow/variables``
     - Akeyless folder path for variables. Set to *None* to disable.
   * - ``config_path``
     - ``/airflow/config``
     - Akeyless folder path for configuration. Set to *None* to disable.
   * - ``sep``
     - ``/``
     - Separator between base path and key name.
   * - ``api_url``
     - ``https://api.akeyless.io``
     - Akeyless API endpoint.
   * - ``access_id``
     -
     - Akeyless Access ID.
   * - ``access_key``
     -
     - Akeyless Access Key (for ``api_key`` auth).
   * - ``access_type``
     - ``api_key``
     - Authentication method.
