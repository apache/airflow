..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

gRPC
~~~~~~~~~~~~~~~~~~~~~

The gRPC connection type enables integrated connection to a gRPC service

Authenticating to gRPC
'''''''''''''''''''''''

There are several ways to connect to gRPC service using Airflow.

1. Using `NO_AUTH` mode, simply setup an insecure channel of connection.
2. Using `SSL` or `TLS` mode, supply a credential pem file for the connection id,
   this will setup SSL or TLS secured connection with gRPC service.
3. Using `JWT_GOOGLE` mode. It is using google auth default credentials by default,
   further use case of getting credentials from service account can be add later on.
4. Using `OATH_GOOGLE` mode. Scopes are required in the extra field, can be setup in the UI.
   It is using google auth default credentials by default,
   further use case of getting credentials from service account can be add later on.
5. Using `CUSTOM` mode. For this type of connection, you can pass in a connection
   function takes in the connection object and return a gRPC channel and supply whatever
   authentication type you want.

Default Connection IDs
''''''''''''''''''''''

The following connection IDs are used by default.

``grpc_default``
    Used by the :class:`~airflow.contrib.hooks.grpc_hook.GrpcHook`
    hook.

Configuring the Connection
''''''''''''''''''''''''''

Host
    The host url of the gRPC server

Port (Optional)
    The port to connect to on gRPC server

Auth Type
    Authentication type of the gRPC connection.
    `NO_AUTH` by default, possible values are
    `NO_AUTH`, `SSL`, `TLS`, `JWT_GOOGLE`,
    `OATH_GOOGLE`, `CUSTOM`

Credential Pem File (Optional)
    Pem file that contains credentials for
    `SSL` and `TLS` type auth
    Not required for other types.

Scopes (comma separated) (Optional)
    A list of comma-separated `Google Cloud scopes
    <https://developers.google.com/identity/protocols/googlescopes>`_ to
    authenticate with.
    Only for `OATH_GOOGLE` type connection