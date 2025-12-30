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

Changelog

1.2.0
.....

New Features
~~~~~~~~~~~~

* **Connection Pooling** - Implemented connection pooling with ``PooledThriftStrategy`` for high-throughput operations
* **Batch Operation Optimization** - Enhanced batch operations with chunking, parallel processing, and configurable batch sizes
* **Performance Improvements** - Significant performance improvements for bulk data operations
* **Global Pool Management** - Added global connection pool storage to prevent DAG hanging issues
* **Backpressure Control** - Implemented backpressure mechanisms for stable batch processing

Enhancements
~~~~~~~~~~~~

* **Simplified Connection Pooling** - Reduced connection pool implementation from ~200 lines to ~40 lines by using built-in happybase.ConnectionPool
* **Configurable Batch Sizes** - Added ``batch_size`` parameter to batch operators (default: 200 rows)
* **Parallel Processing** - Added ``max_workers`` parameter for multi-threaded batch operations (default: 4 workers)
* **Thread Safety** - Improved thread safety with proper connection pool validation
* **Data Size Monitoring** - Added data size monitoring and logging for batch operations
* **Connection Strategy Selection** - Automatic selection between ThriftStrategy and PooledThriftStrategy based on configuration

Operator Updates
~~~~~~~~~~~~~~~~

* ``HBaseBatchPutOperator`` - Added ``batch_size`` and ``max_workers`` parameters for optimized bulk inserts
* Enhanced batch operations with chunking support for large datasets
* Improved error handling and retry logic for batch operations

Connection Configuration
~~~~~~~~~~~~~~~~~~~~~~~

* Added ``pool_size`` parameter to enable connection pooling (default: 1, no pooling)
* Added ``pool_timeout`` parameter for connection pool timeout (default: 30 seconds)
* Added ``batch_size`` parameter for default batch operation size (default: 200)
* Added ``max_workers`` parameter for parallel processing (default: 4)

Performance
~~~~~~~~~~~

* Connection pooling provides up to 10x performance improvement for concurrent operations
* Batch operations optimized with chunking and parallel processing
* Reduced memory footprint through efficient connection reuse
* Improved throughput for high-volume data operations

Bug Fixes
~~~~~~~~~

* Fixed DAG hanging issues by implementing proper connection pool reuse
* Resolved connection leaks in batch operations
* Fixed thread safety issues in concurrent access scenarios
* Improved connection cleanup and resource management

1.1.0
.....

New Features
~~~~~~~~~~~~

* **SSL/TLS Support** - Added comprehensive SSL/TLS support for Thrift connections with certificate validation
* **Kerberos Authentication** - Implemented Kerberos authentication with keytab support
* **Secrets Backend Integration** - Added support for storing SSL certificates and keytabs in Airflow Secrets Backend
* **Enhanced Security** - Automatic masking of sensitive data (passwords, keytabs, tokens) in logs
* **Backup Operations** - Added operators for HBase backup and restore operations
* **Connection Pooling** - Improved connection management with retry logic
* **Connection Strategies** - Support for both Thrift and SSH connection modes
* **Error Handling** - Comprehensive error handling and logging
* **Example DAGs** - Complete example DAGs demonstrating all functionality

Operators
~~~~~~~~~

* ``HBaseCreateBackupOperator`` - Create full or incremental HBase backups
* ``HBaseRestoreOperator`` - Restore HBase tables from backups
* ``HBaseBackupSetOperator`` - Manage HBase backup sets
* ``HBaseBackupHistoryOperator`` - Query backup history and status

Security Enhancements
~~~~~~~~~~~~~~~~~~~~

* ``SSLHappyBaseConnection`` - Custom SSL-enabled HBase connection class
* ``KerberosAuthenticator`` - Kerberos authentication with automatic ticket renewal
* ``HBaseSecurityMixin`` - Automatic masking of sensitive data in logs and output
* Certificate management through Airflow Variables and Secrets Backend

Bug Fixes
~~~~~~~~~

* Improved error handling and connection retry logic
* Fixed connection cleanup and resource management
* Enhanced compatibility with different HBase versions


1.0.0
.....

Initial version of the provider.

Features
~~~~~~~~

* ``HBaseHook`` - Hook for connecting to Apache HBase via Thrift
* ``HBaseCreateTableOperator`` - Operator for creating HBase tables with column families
* ``HBaseDeleteTableOperator`` - Operator for deleting HBase tables
* ``HBasePutOperator`` - Operator for inserting single rows into HBase tables
* ``HBaseBatchPutOperator`` - Operator for batch inserting multiple rows
* ``HBaseBatchGetOperator`` - Operator for batch retrieving multiple rows by keys
* ``HBaseScanOperator`` - Operator for scanning HBase tables with filters
* ``HBaseTableSensor`` - Sensor for checking HBase table existence
* ``HBaseRowSensor`` - Sensor for checking specific row existence
* ``HBaseRowCountSensor`` - Sensor for monitoring row count thresholds
* ``HBaseColumnValueSensor`` - Sensor for checking specific column values
* ``hbase_table_dataset`` - Dataset support for HBase tables in Airflow lineage
* **Authentication** - Basic authentication support for HBase Thrift servers
