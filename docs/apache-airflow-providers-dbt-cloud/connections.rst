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

.. spelling::

    getdbt


.. _howto/connection:dbt-cloud:

Connecting to dbt Cloud
=======================

After installing the dbt Cloud provider in your Airflow environment, the corresponding connection type of
``dbt_cloud`` will be made available. The following describes how to configure an API token and optionally
provide an Account ID and/or a Tenant name for your dbt Cloud connection.

Default Connection ID
~~~~~~~~~~~~~~~~~~~~~

All hooks and operators related to dbt Cloud use ``dbt_cloud_default`` by default.


Authenticating to the dbt Cloud API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To interact with the dbt Cloud API in Airflow, either a
`User API Token <https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/user-tokens>`__ or a
`Service Account API Token <https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/service-tokens>`__ is
required.


Configuring the connection
~~~~~~~~~~~~~~~~~~~~~~~~~~

Password (required)
    The API token to use when authenticating to the dbt Cloud API.

    If using the Connection form in the Airflow UI, the token can also be stored in the "API Token" field.

Login (optional)
    The Account ID to be used as the default Account ID for dbt Cloud operators or
    :class:`~airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook` methods. If an Account ID is provided in the
    connection, you are not required to pass ``account_id`` to operators or hook methods. The ``account_id``
    value will be retrieved from the Airflow connection instead. If needed, the ``account_id`` can still be
    explicitly passed to an operator or hook method as an override the default value configured in the
    connection.

    If using the Connection form in the Airflow UI, the Account ID can also be stored in the "Account ID"
    field.

    .. note::

      If an Account ID is not provided in an Airflow connection, ``account_id`` *must* be explicitly passed to
      an operator or hook method.

Host (optional)
    The Tenant domain for your dbt Cloud environment (e.g. "my-tenant.getdbt.com"). This is particularly
    useful when using a single-tenant dbt Cloud instance or `other dbt Cloud regions <https://docs.getdbt.com/docs/deploy/regions-ip-addresses>`__
    like EMEA or a Virtual Private dbt Cloud. If a Tenant domain is not provided, "cloud.getdbt.com" will be
    used as the default value assuming a multi-tenant instance in the North America region.

    If using the Connection form in the Airflow UI, the Tenant domain can also be stored in the "Tenant"
    field.

When specifying the connection as an environment variable, you should specify it following the standard syntax
of a database connection. Note that all components of the URI should be URL-encoded.


For example, to add a connection with the connection ID of "dbt_cloud_default":

    When specifying an Account ID:

    .. code-block:: bash

        export AIRFLOW_CONN_DBT_CLOUD_DEFAULT='dbt-cloud://account_id:api_token@'

    When *not* specifying an Account ID:

    .. code-block:: bash

        export AIRFLOW_CONN_DBT_CLOUD_DEFAULT='dbt-cloud://:api_token@'

    When specifying a Tenant domain:

    .. code-block:: bash

        export AIRFLOW_CONN_DBT_CLOUD_DEFAULT='dbt-cloud://:api_token@my-tenant.getdbt.com'

You can refer to the documentation on
:ref:`creating connections via environment variables <environment_variables_connections>` for more
information.
