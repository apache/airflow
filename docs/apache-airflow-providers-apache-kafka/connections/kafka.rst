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


.. _howto/connections:kafka:

Kafka Connection
================
The Kafka connection type provides connection to a Apache Kafka cluster.

Configuring the Connection
--------------------------

Host (required)
    The host to connect to. By default ``localhost``.
    Might be a single host (example: ``host1.com``)
    or comma-separated list of hosts (example: ``host1.com,host2.com``)
    or comma-separated list of hosts with ports (example: ``host1.com:9092,host2.com:9092``).

Port (optional)
    The port to connect to. By default ``9092``.
    If host field contains single host or comma-separated list of hosts without ports
    then port will be used for each host.

Schema (required)
    Specify the **security protocol** to communicate with brokers. Valid values are:

    * ``PLAINTEXT`` - Un-authenticated, non-encrypted channel.
    * ``SASL_PLAINTEXT`` - SASL authenticated, non-encrypted channel.
    * ``SASL_SSL`` - SASL authenticated, SSL channel.
    * ``SSL`` - SSL channel.

Login (optional)
    Specify the login to connect. It depends on security protocol.

    - ``PLAINTEXT``: leave empty.
    - ``SASL_PLAINTEXT``: in addition depends on ``sasl_mechanism``

      * if "PLAIN" or one of "SCRAM" then put the ``sasl_plain_username`` value here;
      * if "GSSAPI" (kerberos) or "OAUTHBEARER" then leave empty;

    - ``SASL_SSL``: in addition depends on ``sasl_mechanism``

      * if "PLAIN" or one of "SCRAM" then put the ``sasl_plain_username`` value here;
      * if "GSSAPI" (kerberos) or "OAUTHBEARER" then leave empty;

    - ``SSL``: put the ``ssl_keyfile`` value here.

Password (optional)
    Specify the secret which used to connect. It depends on authentication provider.

    - ``PLAINTEXT``: leave empty.
    - ``SASL_PLAINTEXT``: in addition depends on ``sasl_mechanism``

      * if "PLAIN" or one of "SCRAM" then put the ``sasl_plain_password`` value here;
      * if "OAUTHBEARER" then put token value here;
      * if "GSSAPI" (kerberos) then leave empty;

    - ``SASL_SSL``: in addition depends on ``sasl_mechanism``

      * if "PLAIN" or one of "SCRAM" then put the ``sasl_plain_password`` value here;
      * if "OAUTHBEARER" then put token value here;
      * if "GSSAPI" (kerberos) then leave empty;

    - ``SSL``: put the ``ssl_password`` value here.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used for Kafka classes.
    The following parameters are supported:

    Connection params:

    * ``request_timeout_ms`` - Client request timeout in milliseconds. By default ``30000``
    * ``connections_max_idle_ms`` - . By default ``9 * 60 * 1000``
    * ``reconnect_backoff_ms`` - The amount of time in milliseconds to wait before attempting to reconnect to a given host. By default ``50``
    * ``reconnect_backoff_max_ms`` - The maximum amount of time in milliseconds to backoff/wait when reconnecting to a broker that has repeatedly failed to connect. By default ``1000``
    * ``retry_backoff_ms`` - . By default ``100``
    * ``metadata_max_age_ms`` - . By default ``300000``
    * ``max_in_flight_requests_per_connection`` - Requests are in pipeline to Apache Kafka brokers up to this number of maximum requests per broker connection. By default ``5``
    * ``receive_buffer_bytes`` - The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. By default ``None``
    * ``send_buffer_bytes`` - The size of the TCP send buffer (SO_SNDBUF) to use when sending data. By default ``None``
    * ``api_version_auto_timeout_ms`` - Number of milliseconds to throw a timeout exception from the constructor when checking the broker api version. By default ``2000``
    * ``api_version`` - Specify which Kafka API version to use. Accepted values are: ``(0, 8, 0), (0, 8, 1), (0, 8, 2), (0, 9), (0, 10)``. Default: ``(0, 8, 2)``.
    * ``metrics_num_samples`` - . By default ``2``
    * ``metrics_sample_window_ms`` - . By default ``30000``

    Additional params for SASL.
    Specify only when security protocol is ``SASL_PLAINTEXT`` or ``SASL_SSL``.

    * ``sasl_mechanism`` – Authentication mechanism. Valid values are: ``PLAIN``, ``GSSAPI``, ``OAUTHBEARER``, ``SCRAM-SHA-256``, ``SCRAM-SHA-512``.
    * ``sasl_plain_username`` – username for sasl authentication. Required for ``PLAIN`` or one of the ``SCRAM`` mechanisms. Is taken from login field.
    * ``sasl_plain_password`` – password for sasl authentication. Required for ``PLAIN`` or one of the ``SCRAM`` mechanisms. Is taken from password field.
    * ``sasl_kerberos_service_name`` – Service name to include in ``GSSAPI`` sasl mechanism handshake. Default: ``kafka``.
    * ``sasl_kerberos_domain_name`` – kerberos domain name to use in ``GSSAPI`` sasl mechanism handshake. Default: one of bootstrap servers.
    * ``sasl_oauth_token_provider`` – ``OAUTHBEARER`` token provider instance. Internally implemented class that returns value of password field.

    Additional params for SSL:
    Specify only when security protocol is ``SSL`` or ``SASL_SSL``.

    * ``ssl_check_hostname`` – flag to configure whether ssl handshake should verify that the certificate matches the brokers hostname. By default: ``True``.
    * ``ssl_cafile`` – optional filename of ca file to use in certificate verification. By default: ``None``.
    * ``ssl_certfile`` – optional filename of file in pem format containing the client certificate, as well as any ca certificates needed to establish the certificate’s authenticity. By default: ``None``.
    * ``ssl_crlfile`` – optional filename containing the CRL to check for certificate expiration. By default, no CRL check is done, ``None``.
    * ``ssl_ciphers`` – optionally set the available ciphers for ssl connections. It should be a string in the OpenSSL cipher list format. By default ``None``.
    * ``ssl_keyfile`` – optional filename containing the client private key. Is taken from ``login`` field for ``SSL`` protocol.
    * ``ssl_password`` – optional password to decrypt the client private key. Is taken from ``password`` field for ``SSL`` protocol.


    More details about these Kafka parameters can be found in
    `Python API documentation <https://kafka-python.readthedocs.io/en/master/apidoc/BrokerConnection.html>`_.
    Example "extras" field:

    .. code-block:: json

        {
        "ssl_check_hostname": true,
        "ssl_certfile": "/tmp/client-cert.pem"
        }

    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded), like
    ``kafka://login:PASSWORD@1.1.1.1:9092/schema?extra_param=smth``

    .. code-block:: bash

        export AIRFLOW_CONN_KAFKA_DEFAULT='kafka://:TOKEN@1.1.1.1:9092/SASL_PLAINTEXT?ssl_certfile=%2Ftmp%2Fclient-cert.pem'

    But some parameters for the client should be ``int`` or ``bool``, therefore
    put URL-encoded json to ``__extra__`` parameter:

    .. code-block:: bash

        export AIRFLOW_CONN_KAFKA_DEFAULT=kafka://:TOKEN@1.1.1.1:9092/SASL_PLAINTEXT?__extra__=%7B%22request_timeout_ms%22%3A45%2C%22ssl_check_hostname%22%3A%20true%7D

    More in `library documentation <https://kafka-python.readthedocs.io/en/master/usage.html>`_
