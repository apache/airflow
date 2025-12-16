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
Host (required)
    The host to connect to HBase Thrift server.

Port (optional)
    The port to connect to HBase Thrift server. Default is 9090.

Extra (optional)
    The extra parameters (as json dictionary) that can be used in HBase
    connection. The following parameters are supported:

    * ``timeout`` - Socket timeout in milliseconds. Default is None (no timeout).
    * ``autoconnect`` - Whether to automatically connect when creating the connection. Default is True.
    * ``table_prefix`` - Prefix to add to all table names. Default is None.
    * ``table_prefix_separator`` - Separator between table prefix and table name. Default is b'_' (bytes).
    * ``compat`` - Compatibility mode for older HBase versions. Default is '0.98'.
    * ``transport`` - Transport type ('buffered', 'framed'). Default is 'buffered'.
    * ``protocol`` - Protocol type ('binary', 'compact'). Default is 'binary'.

Examples for the **Extra** field
--------------------------------

1. Specifying timeout and transport options

.. code-block:: json

    {
      "timeout": 30000,
      "transport": "framed",
      "protocol": "compact"
    }

2. Specifying table prefix

.. code-block:: json

    {
      "table_prefix": "airflow",
      "table_prefix_separator": "_"
    }

3. Compatibility mode for older HBase versions

.. code-block:: json

    {
      "compat": "0.96",
      "autoconnect": false
    }

.. seealso::
    https://pypi.org/project/happybase/