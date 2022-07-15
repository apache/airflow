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

OpenMLDB Connection
===================

Default Connection IDs
----------------------

The default connection ID is ``openmldb_default``.

Configuring the Connection
--------------------------

Host (optional)
    Specify the entire url or the base of the url for the service.

Port (optional)
    Specify a port number if applicable.

Schema (optional)
    Specify the service type etc: only http.

Extras (optional)
    Specify headers in json format.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_OPENMLDB_DEFAULT='http://localhost:9080/'
