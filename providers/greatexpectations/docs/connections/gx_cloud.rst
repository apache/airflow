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



.. _howto/connection:gx_cloud:

Great Expectations Cloud Connection
====================================

The Great Expectations Cloud connection type enables integration with `GX Cloud <https://greatexpectations.io/cloud>`__,
the managed backend for Great Expectations that provides a hosted platform for data quality validation and monitoring.

Authenticating to GX Cloud
---------------------------

To authenticate with GX Cloud, you need to obtain your Cloud credentials from your GX Cloud account.
See the `GX Cloud documentation <https://docs.greatexpectations.io/docs/cloud/connect/connect_python#get-your-user-access-token-and-organization-id>`__
for instructions on how to get your user access token and organization ID.

Default Connection IDs
----------------------

The Great Expectations Cloud hook uses ``gx_cloud_default`` by default.

Configuring the Connection
---------------------------

Login (required)
    Specify the GX Cloud Organization ID.

Schema (required)
    Specify the GX Cloud Workspace ID.

Password (required)
    Specify the GX Cloud Access Token.

.. note::

    The following fields are hidden in the connection form as they are not used for GX Cloud connections:
    ``host``, ``port``, ``extra``.

URI format example
^^^^^^^^^^^^^^^^^^

When specifying the connection using an environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

.. code-block:: bash

   export AIRFLOW_CONN_GX_CLOUD_DEFAULT='gx-cloud://organization_id@?password=access_token&schema=workspace_id'

JSON format example
^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   export AIRFLOW_CONN_GX_CLOUD_DEFAULT='{
       "conn_type": "gx_cloud",
       "login": "your_organization_id",
       "schema": "your_workspace_id",
       "password": "your_access_token"
   }'
