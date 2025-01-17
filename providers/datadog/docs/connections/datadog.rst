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

.. _howto/connection:kubernetes:

Datadog Connection
=============================

The Datadog Connection type enables integrations with the Datadog API client.


Default Connection IDs
----------------------

Hooks and sensors related to Datadog use ``datadog_default`` by default.

Configuring the Connection
--------------------------
Host
    Host name to associate with the sent events.

Extra
    Specify the extra parameters (as a JSON dictionary) that can be used in Datadog
    connection.

    ``api_host``: Datadog `API endpoint <https://docs.datadoghq.com/getting_started/site/#access-the-datadog-site>`__

    ``api_key``: Datadog `API key <https://docs.datadoghq.com/account_management/api-app-keys/#api-keys>`__

    ``app_key``: Datadog `application key <https://docs.datadoghq.com/account_management/api-app-keys/#application-keys>`__

    ``source_type_name``: Datadog `source type name <https://docs.datadoghq.com/integrations/faq/list-of-api-source-attribute-value/>`__ (defaults to my_apps).


Secret Management
-----------------

When storing the connection details in a secret management system, it can be convenient to name the secret with the default value::

  secret name: airflow/connections/datadog_default

The following json is an example of what the secret contents should look like::

  {
    "conn_type": "datadog",
    "description": "Datadog connection for my app",
    "extra": {
      "api_host": "https://api.datadoghq.com",
      "api_key": "my api key",
      "app_key": "my app key",
      "source_type_name": "apache"
    },
    "host": "environment-region-application.domain.com"
  }
