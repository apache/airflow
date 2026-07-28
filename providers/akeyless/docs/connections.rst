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

Akeyless Connection
===================

The ``akeyless`` connection type enables connecting to
`Akeyless Vault Platform <https://www.akeyless.io/>`__.

Configuring the Connection
--------------------------

Host
    Akeyless API URL (e.g. ``https://api.akeyless.io`` or your Gateway URL on
    port ``8081``).

Login
    Your Akeyless **Access ID** (e.g. ``p-xxxxxxxxx``).

Password
    Your Akeyless **Access Key** (for ``api_key`` authentication type).

Extra (JSON)
    Provide additional fields depending on your authentication method:

    .. list-table::
       :header-rows: 1

       * - Field
         - Description
       * - ``access_type``
         - Authentication method.  One of: ``api_key`` (default), ``aws_iam``,
           ``gcp``, ``azure_ad``, ``uid``, ``jwt``, ``k8s``, ``certificate``.
       * - ``uid_token``
         - Universal-Identity token (for ``uid`` auth).
       * - ``gcp_audience``
         - GCP audience string (for ``gcp`` auth).
       * - ``azure_object_id``
         - Azure AD Object ID (for ``azure_ad`` auth).
       * - ``jwt``
         - Raw JWT token (for ``jwt`` auth).
       * - ``k8s_auth_config_name``
         - Kubernetes Auth Config name (for ``k8s`` auth).
       * - ``certificate_data``
         - PEM-encoded client certificate (for ``certificate`` auth).
       * - ``private_key_data``
         - PEM-encoded private key (for ``certificate`` auth).

Examples
--------

**API Key authentication (default)**:

.. code-block:: json

    {
        "access_type": "api_key"
    }

With ``Login`` = Access ID and ``Password`` = Access Key.

**AWS IAM authentication**:

.. code-block:: json

    {
        "access_type": "aws_iam"
    }

Requires the ``akeyless_cloud_id`` package (``pip install apache-airflow-providers-akeyless[cloud_id]``).

**Kubernetes authentication**:

.. code-block:: json

    {
        "access_type": "k8s",
        "k8s_auth_config_name": "my-k8s-config"
    }
