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

Apache Trino Connection
=======================

The Apache Trino connection type enables connection to Trino which is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.

Default Connection IDs
----------------------

Trino Hook uses the parameter ``trino_conn_id`` for Connection IDs and the value of the parameter as ``trino_default`` by default.
Trino Hook supports multiple authentication types to ensure all users of the system are authenticated, the parameter ``auth`` can be set to enable authentication. The value of the parameter is ``None`` by default.

Configuring the Connection
--------------------------
Host
    The host to connect to, it can be ``local``, ``yarn`` or an URL.

Port
    Specify the port in case of host is an URL.

Login
    Effective user for connection.

Password
    This can be to pass to enable Basic Authentication. This is an optional parameter and is not required if a different authentication mechanism is used.

Extra (optional, connection parameters)
    Specify the extra parameters (as json dictionary) that can be used in Trino connection. The following parameters out of the standard python parameters are supported:

    * ``auth`` - Specifies which type of authentication needs to be enabled. The value can be ``certs``, ``kerberos``, or ``jwt``
    * ``impersonate_as_owner`` - Boolean that allows to set ``AIRFLOW_CTX_DAG_OWNER`` as a user of the connection.

    The following extra parameters can be used to configure authentication:

    * ``jwt__token`` - If jwt authentication should be used, the value of token is given via this parameter.
    * ``certs__client_cert_path``, ``certs__client_key_path``- If certificate authentication should be used, the path to the client certificate and key is given via these parameters.
    * ``kerberos__service_name``, ``kerberos__config``, ``kerberos__mutual_authentication``, ``kerberos__force_preemptive``, ``kerberos__hostname_override``, ``kerberos__sanitize_mutual_error_response``, ``kerberos__principal``,``kerberos__delegate``, ``kerberos__ca_bundle`` - These parameters can be set when enabling ``kerberos`` authentication.
    * ``session_properties`` - JSON dictionary which allows to set session_properties. Example: ``{'session_properties':{'scale_writers':true,'task_writer_count:1'}}``
