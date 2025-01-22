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

.. _howto/connection:iceberg:

Connecting to Iceberg
=====================

The Iceberg connection type enables connecting to an Iceberg REST catalog to request a short-lived token to access the Apache Iceberg tables. This token can be injected as an environment variable, to be used with Trino, Spark, Flink, or your favorite query engine that supports Apache Iceberg.

After installing the Iceberg provider in your Airflow environment, the corresponding connection type of ``iceberg`` will be made available.

Default Connection IDs
----------------------

Iceberg Hook uses the parameter ``iceberg_conn_id`` for Connection IDs and the value of the parameter as ``iceberg_default`` by default. You can create multiple connections in case you want to switch between environments.

Configuring the Connection
--------------------------

Client ID
    The OAuth2 Client ID

Client Secret
    The OAuth2 Client Secret

Host
    Sets the URL to the Tabular environment. By default `https://api.tabulardata.io/ws/v1`
