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

Configuring the Connection
--------------------------

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
        hbase_conn_id="hbase_default",  # HBase Thrift connection
        ssh_conn_id="hbase_ssh",        # SSH connection for shell commands
    )

.. note::
    For backup and restore operations to work correctly, the SSH connection **must** include ``hbase_home`` and ``java_home`` in the Extra field. These parameters allow the hook to locate the HBase binaries and set the correct Java environment on the remote server.

.. seealso::
    https://pypi.org/project/happybase/