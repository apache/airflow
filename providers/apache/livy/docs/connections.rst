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

Apache Livy Connection
======================

The Apache Livy connection type enables connection to Apache Livy.
The Apache Livy connection uses the Http connection under the hood.

Default Connection IDs
----------------------

Livy Hook uses parameter ``livy_conn_id`` for Connection IDs and the value of the
parameter as ``livy_default`` by default.

Configuring the Connection
--------------------------
Host
    The http host of the Livy server. You may add the scheme in the ``Host`` field or give it in the ``Schema`` field.

Port
    Specify the port in case of host be an URL of the Apache Livy server.

Schema (optional)
    Specify the service type etc: http/https.

Login (optional)
    Specify the login for the Apache Livy server you would like to connect too.

Password (optional)
    Specify the password for the Apache Livy server you would like to connect too.

Extra (optional)
    Specify headers in json format.

When specifying the connection in environment variable you should specify
it using URI syntax.

Note that all components of the URI should be URL-encoded.

For example:

.. code-block:: bash

   export AIRFLOW_CONN_LIVY_DEFAULT='livy://username:password@livy-server.com:80/http?headers=header'
