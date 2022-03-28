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



.. _howto/connection:delta_sharing:

Delta Sharing Connection
========================

The Delta Sharing connection type enables the integration with a Delta Sharing server.

Authenticating to Delta Sharing endpoint
----------------------------------------

Authentication to Delta Sharing endpoint is performed using URL & bearer token that you
need to extract from a JSON file you downloaded.

Default Connection IDs
----------------------

Hooks and operators related to Delta Sharing use ``delta_sharing_default`` by default.

Configuring the Connection
--------------------------

Host (required)
    Specify the base URL of Delta Sharing (``endpoint`` field in the JSON file you received)

Password (required)
    * Bearer token that will be used for authentication (``bearerToken`` field in the JSON file you received)


When specifying the connection using an environment variable you should specify it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_DELTA_SHARING_DEFAULT='delta_sharing://@host-url?password=bearerToken'
