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


.. _howto/connections:pulsar:

Pulsar Connection
=================
The Pulsar connection type provides connection to a Apache Pulsar instance.

Configuring the Connection
--------------------------

Host (required)
    The host to connect to. By default ``localhost``.
    Might be a single host (example: ``host1.com``)
    or comma-separated list of hosts (example: ``host1.com,host2.com``)
    or comma-separated list of hosts with ports (example: ``host1.com:6650,host2.com:6651``).

Port (optional)
    The port to connect to. By default ``6650``.
    If host field contains single host or comma-separated list of hosts without ports
    then port will be used for each host.

Schema (optional)
    Specify the authentication provider. Supported providers:

    * ``tls``
    * ``jwt`` (JSON Web Tokens)
    * ``athenz``
    * ``oauth``

    Leave this field empty if no authentication is needed.

Login (optional)
    Specify the login to connect. It depends on authentication provider.

    - ``tls``: put the ``certificate_path`` value here.
    - ``jwt``: leave empty.
    - ``athenz``: put ``auth_params`` string here. Example:

      .. code-block:: json

          {
          "tenantDomain": "shopping",
          "tenantService": "some_app",
          "providerDomain": "pulsar",
          "privateKey": "file:///path/to/private.pem",
          "keyId": "v1"
          }

    - ``oauth``: put ``auth_params`` string here. Example:

      .. code-block:: json

          {
          "issuer_url": "https://dev-kt-aa9ne.us.auth0.com",
          "private_key": "/path/to/privateKey",
          "audience": "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
          }

Password (optional)
    Specify the secret which used to connect. It depends on authentication provider.

    - ``tls``: put the ``private_key_path`` value here.
    - ``jwt``: put the token value here.
    - ``athenz``: leave empty.
    - ``oauth``: leave empty.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Pulsar
    client. The following parameters are supported:

    * ``operation_timeout_seconds`` - Set timeout on client operations (subscribe, create producer, close, unsubscribe). By default ``30``
    * ``io_threads`` - Set the number of IO threads to be used by the Pulsar client. By default ``1``
    * ``message_listener_threads`` - Set the number of threads to be used by the Pulsar client when delivering messages through message listener. By default ``1``
    * ``concurrent_lookup_requests`` - Number of concurrent lookup-requests allowed on each broker connection to prevent overload on the broker. By default ``50000``
    * ``log_conf_file_path`` - Initialize ``Apache log4cxx`` from a configuration file. By default ``None``
    * ``use_tls`` - Configure whether to use TLS encryption on the connection. By default ``False``
    * ``tls_trust_certs_file_path`` - Set the path to the trusted TLS certificate file. By default ``None``
    * ``tls_allow_insecure_connection`` - Configure whether the Pulsar client accepts not trusted TLS certificates from the broker. By default ``False``
    * ``tls_validate_hostname`` - Configure whether the Pulsar client validates that the hostname of the endpoint. By default ``False``
    * ``connection_timeout_ms`` - Set timeout in milliseconds on TCP connections. By default ``10000``

    More details about these Pulsar parameters can be found in
    `Python API documentation <https://pulsar.apache.org/api/python/#pulsar.Client>`_.
    Example "extras" field:
    .. code-block:: json

        {
        "operation_timeout_seconds": 60
        "connection_timeout_ms": 30000
        "use_tls": true,
        "tls_trust_certs_file_path": "/tmp/client-cert.pem"
        }

    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded), like
    ``pulsar://login:PASSWORD@1.1.1.1:6650/schema?extra_param=smth``

    .. code-block:: bash

        export AIRFLOW_CONN_PULSAR_DEFAULT='pulsar://:TOKEN@1.1.1.1:6650/jwt?tls_trust_certs_file_path=%2Ftmp%2Fclient-cert.pem'

    But some parameters for the client should be ``int`` or ``bool``, therefore
    put URL-encoded json to ``__extra__`` parameter:

    .. code-block:: bash

        export AIRFLOW_CONN_PULSAR_DEFAULT=pulsar://:TOKEN@1.1.1.1:6650/jwt?__extra__=%7B%22operation_timeout_seconds%22%3A45%2C%22use_tls%22%3A%20true%7D

    More about `Python client <https://pulsar.apache.org/docs/en/client-libraries-python/>`_
    and `library documentation <https://pulsar.apache.org/api/python/>`_
