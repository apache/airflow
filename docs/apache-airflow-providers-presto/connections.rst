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

Presto Connection
=================

The Presto connection type enables connection to Presto which is an open-source distributed SQL query engine designed for fast analytics on large-scale data sources, enabling interactive querying across multiple data platforms.


Default Connection IDs
----------------------

Presto Hook uses the parameter ``presto_conn_id`` for Connection IDs and the value of the parameter as ``presto_default`` by default.
Presto Hook supports multiple authentication types to ensure all users of the system are authenticated, the parameter ``auth`` can be set to enable authentication. The value of the parameter is ``None`` by default.

Configuring the Connection
--------------------------
Host
    The host to connect to.

Port
    The port to connect to the host. Presto will use 8080 as default.

Login
    Effective user for connection.

Password
    This can be to pass to enable Basic Authentication. This is an optional parameter and is not required if a different authentication mechanism is used.

Extra (optional, connection parameters)
    Specify the extra parameters (as json dictionary) that can be used in Presto connection. The following parameters out of the standard python parameters are supported:

    * ``auth`` - Specifies which type of authentication needs to be enabled. The value can be ``kerberos``.
    * ``source`` - Specifies source to connect to. Default value is ``airflow``.
    * ``protocol`` - Specifies port to connect with. Default value is ``http``.
    * ``catalog`` - Specifies which catalog to use. Default value is ``hive``.
    * ``verify`` - Client certificate path to connect with SSL/TLS.


    The following extra parameters can be used to configure authentication:

    * ``kerberos__service_name``, ``kerberos__config``, ``kerberos__mutual_authentication``, ``kerberos__force_preemptive``, ``kerberos__hostname_override``, ``kerberos__sanitize_mutual_error_response``, ``kerberos__principal``, ``kerberos__delegate``, ``kerberos__ca_bundle`` - These parameters can be set when enabling ``kerberos`` authentication.
