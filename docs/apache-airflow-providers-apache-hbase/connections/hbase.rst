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



Apache HBase Connection
=======================

The Apache HBase connection type enables connection to `Apache HBase <https://hbase.apache.org/>`__.

Default Connection IDs
----------------------

HBase hook and HBase operators use ``hbase_default`` by default.

Supported Connection Types
--------------------------

The HBase provider supports multiple connection types for different use cases:

* **hbase** - Direct Thrift connection (recommended for most operations)
* **generic** - Generic connection for Thrift servers
* **ssh** - SSH connection for backup operations and shell commands

Connection Strategies
--------------------

The provider supports two connection strategies for optimal performance:

* **ThriftStrategy** - Single connection for simple operations
* **PooledThriftStrategy** - Connection pooling for high-throughput operations

Connection pooling is automatically enabled when ``pool_size`` is specified in the connection Extra field.
Pooled connections provide better performance for batch operations and concurrent access.

Connection Pool Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable connection pooling, add the following to your connection's Extra field:

.. code-block:: json

    {
      "pool_size": 10,
      "pool_timeout": 30
    }

* ``pool_size`` - Maximum number of connections in the pool (default: 1, no pooling)
* ``pool_timeout`` - Timeout in seconds for getting connection from pool (default: 30)

Connection Examples
-------------------

The following connection examples are based on the provider's test configuration:

Basic Thrift Connection (hbase_thrift)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``generic``
:Host: ``172.17.0.1`` (or your HBase Thrift server host)
:Port: ``9090`` (default Thrift1 port)
:Extra:

.. code-block:: json

    {
      "use_kerberos": false
    }

Pooled Thrift Connection (hbase_pooled)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``generic``
:Host: ``172.17.0.1`` (or your HBase Thrift server host)
:Port: ``9090`` (default Thrift1 port)
:Extra:

.. code-block:: json

    {
      "use_kerberos": false,
      "pool_size": 10,
      "pool_timeout": 30
    }

.. note::
    Connection pooling significantly improves performance for batch operations
    and concurrent access patterns. Use pooled connections for production workloads.

SSL/TLS Connection (hbase_ssl)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``hbase``
:Host: ``172.17.0.1`` (or your SSL proxy host)
:Port: ``9092`` (SSL proxy port, e.g., stunnel)
:Extra:

.. code-block:: json

    {
      "use_ssl": true,
      "ssl_check_hostname": false,
      "ssl_verify_mode": "none",
      "transport": "framed"
    }

Kerberos Connection (hbase_kerberos)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``generic``
:Host: ``172.17.0.1`` (or your HBase Thrift server host)
:Port: ``9090``
:Extra:

.. code-block:: json

    {
      "use_kerberos": true,
      "principal": "hbase_user@EXAMPLE.COM",
      "keytab_secret_key": "hbase_keytab",
      "connection_mode": "ssh",
      "ssh_conn_id": "hbase_ssh",
      "hdfs_uri": "hdfs://localhost:9000"
    }

SSH Connection for Backup Operations (hbase_ssh)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``ssh``
:Host: ``172.17.0.1`` (or your HBase cluster node)
:Port: ``22``
:Login: ``hbase_user`` (SSH username)
:Password: ``your_password`` (or use key-based auth)
:Extra:

.. code-block:: json

    {
      "hbase_home": "/opt/hbase-2.6.4",
      "java_home": "/usr/lib/jvm/java-17-openjdk-amd64",
      "connection_mode": "ssh",
      "ssh_conn_id": "hbase_ssh"
    }

Thrift2 Connection (hbase_thrift2)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:Connection Type: ``generic``
:Host: ``172.17.0.1`` (or your HBase Thrift2 server host)
:Port: ``9091`` (default Thrift2 port)
:Extra:

.. code-block:: json

    {
      "use_ssl": false,
      "transport": "framed"
    }

.. note::
    This connection is typically used as a backend for SSL proxy configurations.
    When using SSL, configure an SSL proxy (like stunnel) to forward encrypted 
    traffic from port 9092 to this plain Thrift2 connection on port 9091.

Configuring the Connection
--------------------------

SSL/TLS Configuration
^^^^^^^^^^^^^^^^^^^^^

SSL Certificate Management
""""""""""""""""""""""""""

The provider supports SSL certificates stored in Airflow's Secrets Backend or Variables:

* ``hbase/ca-cert`` - CA certificate for server verification
* ``hbase/client-cert`` - Client certificate for mutual TLS
* ``hbase/client-key`` - Client private key for mutual TLS

SSL Connection Parameters
"""""""""""""""""""""""""

The following SSL parameters are supported in the Extra field:

* ``use_ssl`` - Enable SSL/TLS (true/false)
* ``ssl_check_hostname`` - Verify server hostname (true/false)
* ``ssl_verify_mode`` - Certificate verification mode:
  
  - ``"none"`` - No certificate verification (CERT_NONE)
  - ``"optional"`` - Optional certificate verification (CERT_OPTIONAL)
  - ``"required"`` - Required certificate verification (CERT_REQUIRED)

* ``ssl_ca_secret`` - Airflow Variable/Secret key containing CA certificate
* ``ssl_cert_secret`` - Airflow Variable/Secret key containing client certificate
* ``ssl_key_secret`` - Airflow Variable/Secret key containing client private key
* ``ssl_min_version`` - Minimum SSL/TLS version (e.g., "TLSv1.2")

SSL Example with Certificate Secrets
"""""""""""""""""""""""""""""""""""""

.. code-block:: json

    {
      "use_ssl": true,
      "ssl_verify_mode": "required",
      "ssl_ca_secret": "hbase/ca-cert",
      "ssl_cert_secret": "hbase/client-cert",
      "ssl_key_secret": "hbase/client-key",
      "ssl_min_version": "TLSv1.2",
      "transport": "framed"
    }

HBase Thrift Connection
^^^^^^^^^^^^^^^^^^^^^^^

For basic HBase operations (table management, data operations), configure the Thrift server connection:

Host (required)
    The host to connect to HBase Thrift server.

Port (optional)
    The port to connect to HBase Thrift server. Default is 9090.

Extra (optional)
    The extra parameters (as json dictionary) that can be used in HBase
    connection. The following parameters are supported:

    **Authentication**
    
    * ``auth_method`` - Authentication method ('simple' or 'kerberos'). Default is 'simple'.
    
    **For Kerberos authentication (auth_method=kerberos):**
    
    * ``principal`` - **Required** Kerberos principal (e.g., 'hbase_user@EXAMPLE.COM').
    * ``keytab_path`` - Path to keytab file (e.g., '/path/to/hbase.keytab').
    * ``keytab_secret_key`` - Alternative to keytab_path: Airflow Variable/Secret key containing base64-encoded keytab.
    
    **Connection parameters:**
    
    * ``timeout`` - Socket timeout in milliseconds. Default is None (no timeout).
    * ``autoconnect`` - Whether to automatically connect when creating the connection. Default is True.
    * ``table_prefix`` - Prefix to add to all table names. Default is None.
    * ``table_prefix_separator`` - Separator between table prefix and table name. Default is b'_' (bytes).
    * ``compat`` - Compatibility mode for older HBase versions. Default is '0.98'.
    * ``transport`` - Transport type ('buffered', 'framed'). Default is 'buffered'.
    * ``protocol`` - Protocol type ('binary', 'compact'). Default is 'binary'.
    
    **Connection pooling parameters:**
    
    * ``pool_size`` - Maximum number of connections in the pool. Default is 1 (no pooling).
    * ``pool_timeout`` - Timeout in seconds for getting connection from pool. Default is 30.
    
    **Batch operation parameters:**
    
    * ``batch_size`` - Default batch size for bulk operations. Default is 200.
    * ``max_workers`` - Maximum number of worker threads for parallel processing. Default is 4.

SSH Connection for Backup Operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For backup and restore operations that require HBase shell commands, you may need to configure an SSH connection.
Create a separate SSH connection with the following parameters:

Connection Type
    SSH

Host (required)
    The hostname of the HBase cluster node where HBase shell commands can be executed.

Username (required)
    SSH username for authentication.

Password/Private Key
    SSH password or private key for authentication.

Extra (required for backup operations)
    Additional SSH and HBase-specific parameters. For backup operations, ``hbase_home`` and ``java_home`` are typically required:

    * ``hbase_home`` - **Required** Path to HBase installation directory (e.g., "/opt/hbase", "/usr/local/hbase").
    * ``java_home`` - **Required** Path to Java installation directory (e.g., "/usr/lib/jvm/java-8-openjdk", "/opt/java").
    * ``timeout`` - SSH connection timeout in seconds.
    * ``compress`` - Enable SSH compression (true/false).
    * ``no_host_key_check`` - Skip host key verification (true/false).
    * ``allow_host_key_change`` - Allow host key changes (true/false).

Examples for the **Extra** field
--------------------------------

1. Simple authentication (default)

.. code-block:: json

    {
      "auth_method": "simple",
      "timeout": 30000,
      "transport": "framed"
    }

2. Kerberos authentication with keytab file

.. code-block:: json

    {
      "auth_method": "kerberos",
      "principal": "hbase_user@EXAMPLE.COM",
      "keytab_path": "/path/to/hbase.keytab",
      "timeout": 30000
    }

3. Kerberos authentication with keytab from secrets

.. code-block:: json

    {
      "auth_method": "kerberos",
      "principal": "hbase_user@EXAMPLE.COM",
      "keytab_secret_key": "hbase_keytab_b64",
      "timeout": 30000
    }

4. Connection with table prefix

.. code-block:: json

    {
      "table_prefix": "airflow",
      "table_prefix_separator": "_",
      "compat": "0.96"
    }

5. Connection with pooling and batch optimization

.. code-block:: json

    {
      "pool_size": 10,
      "pool_timeout": 30,
      "batch_size": 500,
      "max_workers": 8,
      "transport": "framed"
    }

SSH Connection Examples
^^^^^^^^^^^^^^^^^^^^^^^

1. SSH connection with HBase and Java paths

.. code-block:: json

    {
      "hbase_home": "/opt/hbase",
      "java_home": "/usr/lib/jvm/java-8-openjdk",
      "timeout": 30
    }

2. SSH connection with compression and host key settings

.. code-block:: json

    {
      "compress": true,
      "no_host_key_check": true,
      "hbase_home": "/usr/local/hbase"
    }

Using SSH Connection in Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using backup operators, specify the SSH connection ID:

.. code-block:: python

    from airflow.providers.hbase.operators.hbase import HBaseCreateBackupOperator

    backup_task = HBaseCreateBackupOperator(
        task_id="create_backup",
        backup_type="full",
        backup_path="hdfs://namenode:9000/hbase/backup",
        backup_set_name="my_backup_set",
        hbase_conn_id="hbase_ssh",  # SSH connection for backup operations
    )

.. note::
    For backup and restore operations to work correctly, the SSH connection **must** include ``hbase_home`` and ``java_home`` in the Extra field. These parameters allow the hook to locate the HBase binaries and set the correct Java environment on the remote server.

.. seealso::
    https://pypi.org/project/happybase/