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

Analytics Operator
===================

The Analytics operator is designed to run analytic queries on data stored in various datastores. It is a generic operator that can query data in S3, GCS, Azure, and Local File System.

The Analytics Operator uses Apache DataFusion as its query engine and supports SQL as the query language. It operates on a single node engine to deliver high-performance analytics on the data. It can be used for various analytics tasks such as data exploration, data aggregation, and more. `<https://datafusion.apache.org/>`_.

Supported Storage Systems
-------------------------
- S3
- Local File System


Supported File Formats
----------------------
- Parquet
- CSV
- Avro

.. _howto/operator:AnalyticsOperator:

Use the :class:`~airflow.providers.common.ai.operators.analytics.AnalyticsOperator` to run analytic queries.

Parameters
----------
* ``datasource_configs`` (list[DataSourceConfig], required): List of datasource configurations
* ``queries`` (list[str], required): List of SQL queries to run on the data
* ``max_rows_check`` (int, optional): Maximum number of rows to check for each query. Default is 100. If any query returns more than this number of rows, it will be skipped in the results returned by the operator. This is to prevent returning too many rows in the results which can cause xcom rendering issues in Airflow UI.
* ``engine`` (DataFusionEngine, optional): Query engine to use. Default is "datafusion". Currently, only "datafusion" is supported.
* ``result_output_format`` (str, optional): Output format for the results. Default is ``tabulate``. Supported formats are ``tabulate``, ``json``.


S3 Storage
----------
.. exampleinclude:: /../../ai/tests/system/common/ai/example_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_analytics_operator_with_s3]
    :end-before: [END howto_analytics_operator_with_s3]

Local File System Storage
-------------------------
.. exampleinclude:: /../../ai/tests/system/common/ai/example_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_analytics_operator_with_local]
    :end-before: [END howto_analytics_operator_with_local]
