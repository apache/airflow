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

Apache SeaTunnel Connection
===========================

The Apache SeaTunnel connection type enables connection to Apache SeaTunnel.

Default Connection IDs
----------------------

Apache SeaTunnel hooks and operators use ``seatunnel_default`` by default.

Configuring the Connection
--------------------------

Host (required)
    The hostname or IP address of the SeaTunnel server.

Port (optional)
    The port of the SeaTunnel server (default: 8083 for Zeta engine).

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Apache SeaTunnel connection.
    The following parameters out of the standard python parameters are supported:

    * ``seatunnel_home``: Path to the SeaTunnel installation directory (required).

    Example "extras" field:

    .. code-block:: json

       {
          "seatunnel_home": "/path/to/seatunnel"
       }

When specifying the connection in environment variable you should specify it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_SEATUNNEL_DEFAULT='apache-seatunnel://localhost:8083/?seatunnel_home=%2Fpath%2Fto%2Fseatunnel'
