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
---------

1.0.0
.....

Initial version of the provider.

Features
~~~~~~~~

* ``HBaseHook`` - Hook for connecting to Apache HBase via Thrift
* ``HBaseCreateTableOperator`` - Operator for creating HBase tables
* ``HBaseDeleteTableOperator`` - Operator for deleting HBase tables
* ``HBasePutOperator`` - Operator for inserting single rows into HBase
* ``HBaseBatchPutOperator`` - Operator for batch inserting multiple rows
* ``HBaseBatchGetOperator`` - Operator for batch retrieving multiple rows
* ``HBaseScanOperator`` - Operator for scanning HBase tables
* ``HBaseTableSensor`` - Sensor for checking table existence
* ``HBaseRowSensor`` - Sensor for checking row existence
* ``HBaseRowCountSensor`` - Sensor for checking row count thresholds
* ``HBaseColumnValueSensor`` - Sensor for checking column values
* ``hbase_table_dataset`` - Dataset support for HBase tables