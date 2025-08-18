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

==================================================
Configure Airflow to use the Keycloak auth manager
==================================================

To use the Keycloak auth manager in your Airflow environment, you must configure it in your Airflow configuration.

.. note::
  These configuration options must be set on the hosts/environments running the Airflow API server.

.. code-block:: ini

  [core]
  auth_manager = airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager

or

.. code-block:: bash

  export AIRFLOW__CORE__AUTH_MANAGER='airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager'

Additionally, you'll need to provide some Keycloak-specific configuration options.

Config Options
--------------

There are a number of configuration options available, which can either be set directly in the airflow.cfg file under
an ``keycloak_auth_manager`` section or via environment variables using the ``AIRFLOW__KEYCLOAK_AUTH_MANAGER__<OPTION_NAME>`` format,
for example ``AIRFLOW__KEYCLOAK_AUTH_MANAGER__REALM = "Airflow"``. For
more information on how to set these options, see `<https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html>`__.

Required config options:
~~~~~~~~~~~~~~~~~~~~~~~~

- ``client_id``. Client ID configured in Keycloak to integrate with Airflow. This client must follow the standard ``OpenID Connect authentication flow``.
- ``client_secret``. Secret associated to the client configured in Keycloak to integrate with Airflow.
- ``realm``. Realm configured in Keycloak associated to Airflow. This realm define all users, roles, groups and permissions used by Airflow.

Optional config options:
~~~~~~~~~~~~~~~~~~~~~~~~

- ``server_url``. Keycloak server URL. This server URL is used by the Airflow API server to communicate with Keycloak.
  If the Airflow API server and Keycloak are running in Docker, set "http://host.docker.internal:<PORT>" (default value).
  You do not need to set this configuration option if you are running Keycloak with Breeze.
