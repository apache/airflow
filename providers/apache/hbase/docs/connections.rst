

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

.. _howto/connection:hbase:

HBase Connection
================

The HBase connection type enables both Thrift2 and CLI-based communication
with HBase clusters.

Default connection IDs
----------------------

* ``hbase_default`` - default connection used by ``HBaseThriftHook`` and
  ``HBaseCLIHook`` when no explicit ``hbase_conn_id`` is provided.

Connection fields
-----------------

.. list-table::
   :header-rows: 1

   * - Field
     - Description
   * - Host
     - HBase Thrift2 server hostname or IP address.
   * - Port
     - HBase Thrift2 server port (default ``9090``).
   * - Extra
     - JSON dictionary with additional configuration (see below).

Connection extra keys (canonical)
---------------------------------

The following keys are recognized in the ``Connection.extra`` JSON.

**General**

.. list-table::
   :header-rows: 1

   * - Key
     - Type
     - Default
     - Description
   * - ``timeout``
     - int
     - ``30000``
     - Connection timeout in milliseconds.
   * - ``namespace``
     - str
     - ``"default"``
     - HBase namespace for table operations.
   * - ``use_http``
     - bool
     - ``false``
     - Use HTTP transport instead of binary socket (required for SSL
       when ``hbase.regionserver.thrift.http`` is ``true``).

**Authentication**

.. warning::

    **Kerberos-secured clusters are not supported.**

    This provider connects via the Thrift2 protocol over buffered, framed, or HTTP
    transport with optional SSL/TLS. SASL-based authentication (required for
    Kerberos Security) is not implemented. Attempting to connect to a
    Kerberized HBase cluster will result in a connection error

**SSL/TLS**

SSL options can be specified in two formats:

1. Flat: ``{"ca_certs": "...", "cert_file": "...", "key_file": "...", "validate": true}``
2. Nested: ``{"ssl_options": {"ca_certs": "...", ...}}``

.. list-table::
   :header-rows: 1

   * - Key
     - Type
     - Description
   * - ``ca_certs``
     - str
     - Path to CA certificate file or Airflow Variable name containing the cert.
   * - ``cert_file``
     - str
     - Path to client certificate file or Variable name (store in Secret Backend, not plain Variable).
   * - ``key_file``
     - str
     - Path to client key file or Variable name (store in Secret Backend, not plain Variable).
   * - ``validate``
     - bool
     - Whether to validate the server certificate (default: ``true``).

.. note::

    When certificate contents are kept in Airflow Variables, the values are stored in the
    metadata database. Prefer a secret backend (``[secrets] backend``) so certificate material
    is fetched from a managed secret store; secrets registered by Airflow are masked in logs
    and rendered templates.

**Connection pool**

.. list-table::
   :header-rows: 1

   * - Key
     - Type
     - Default
     - Description
   * - ``connection_pool.enabled``
     - bool
     - ``false``
     - Enable Thrift2 connection pooling.
   * - ``connection_pool.size``
     - int
     - ``10``
     - Pool size (max concurrent connections).
   * - ``connection_pool.timeout``
     - int
     - ``30``
     - Timeout in seconds to acquire a connection from the pool.

**Retry**

.. list-table::
   :header-rows: 1

   * - Key
     - Type
     - Default
     - Description
   * - ``retry_max_attempts``
     - int
     - ``3``
     - Maximum number of connection attempts.
   * - ``retry_delay``
     - float
     - ``1.0``
     - Initial delay between retries in seconds.
   * - ``retry_backoff_factor``
     - float
     - ``2.0``
     - Backoff multiplier for sequential retries.

Connection extra templates
--------------------------

**Plain HBase**

.. code-block:: json

    {
        "timeout": 30000,
        "namespace": "default"
    }

**SSL/TLS**

.. code-block:: json

    {
        "use_http": true,
        "ssl_options": {
            "ca_certs": "/etc/ssl/certs/ca-bundle.pem",
            "validate": true
        },
        "timeout": 30000
    }

**Connection pool (high throughput)**

.. code-block:: json

    {
        "connection_pool": {
            "enabled": true,
            "size": 20,
            "timeout": 60
        },
        "timeout": 30000
    }
