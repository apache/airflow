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

Authentication Methods
----------------------

The secrets backend supports the following authentication types:

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - ``access_type``
     - Description
   * - ``api_key``
     - Authenticate with Access ID + Access Key. The default method.
   * - ``uid``
     - Use a pre-existing Universal Identity token.
   * - ``aws_iam``
     - Authenticate using the host's AWS IAM role. Ideal for **Amazon MWAA**,
       EC2, ECS, and EKS workloads. No static credentials required.
   * - ``gcp``
     - Authenticate using GCP workload identity. Ideal for **Google Managed
       Service for Apache Airflow** (formerly Cloud Composer) and GCE/GKE
       workloads.
   * - ``azure_ad``
     - Authenticate using Azure AD identity. Ideal for Azure-hosted workloads.

Cloud-based auth types (``aws_iam``, ``gcp``, ``azure_ad``) require the
optional ``akeyless_cloud_id`` package:

.. code-block:: bash

    pip install apache-airflow-providers-akeyless[cloud_id]

Using with Amazon MWAA
""""""""""""""""""""""

On `Amazon MWAA <https://aws.amazon.com/managed-workflows-for-apache-airflow/>`__
you can leverage the environment's IAM execution role to authenticate with
Akeyless -- no static API keys needed.

1. Add to your ``requirements.txt`` (uploaded to S3):

   .. code-block:: text

       apache-airflow-providers-akeyless[cloud_id]

2. In the MWAA console, add these **Airflow configuration options**:

   .. list-table::
      :widths: 30 70

      * - ``secrets.backend``
        - ``airflow.providers.akeyless.secrets.akeyless.AkeylessBackend``
      * - ``secrets.backend_kwargs``
        - ``{"api_url": "https://api.akeyless.io", "access_id": "p-xxxxxxxxx", "access_type": "aws_iam"}``

3. Ensure the MWAA VPC has outbound HTTPS access to your Akeyless API
   endpoint (``api.akeyless.io`` or your Akeyless Gateway).

4. Create an Akeyless ``aws_iam`` Auth Method associated with the MWAA
   execution role ARN.

Using with Google Managed Service for Apache Airflow
""""""""""""""""""""""""""""""""""""""""""""""""""""

.. code-block:: ini

    [secrets]
    backend = airflow.providers.akeyless.secrets.akeyless.AkeylessBackend
    backend_kwargs = {
        "api_url": "https://api.akeyless.io",
        "access_id": "p-xxxxxxxxx",
        "access_type": "gcp",
        "gcp_audience": "akeyless.io"
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
     - Authentication method (``api_key``, ``uid``, ``aws_iam``, ``gcp``, or ``azure_ad``).
   * - ``gcp_audience``
     -
     - GCP audience string (only for ``gcp`` auth).
   * - ``azure_object_id``
     -
     - Azure AD Object ID (only for ``azure_ad`` auth).
   * - ``token_ttl``
     - ``600``
     - Seconds to cache the API token before re-authenticating.
