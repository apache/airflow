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

.. _datasets-ref:

Dataset URI reference
=====================

This page lists pre-defined dataset URI schemes supported by Airflow core and providers. To understand how to use these URIs, and datasets they support, see :ref:`datasets`.

TODO: Click on each row for more detailed documentation on the individual URI format.

========= ============= ========================================================
Provider  Name          Format
========= ============= ========================================================
--        Local file    ``file://{host}/{path}``
amazon    S3            ``s3://{bucket_name}/{path}``
google    BigQuery      ``bigquery://{project_id}/{dataset}/{table}``
          Cloud Storage ``gcs://{bucket_name}{path}``
mysql     MySQL         ``mysql://{host}:{port}/{database}/{table}``
postgres  PostgreSQL    ``postgres://{host}:{port}/{database}/{schema}/{table}``
trino     Trino         ``trino://{host}:{port}/{catalog}/{schema}/{table}``
========= ============ =========================================================
