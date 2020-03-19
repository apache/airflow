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

Google Cloud BigQuery Operators
===============================

`BigQuery <https://cloud.google.com/bigquery/>`__ is Google's fully managed, petabyte 
scale, low cost analytics data warehouse. It is a serverless Software as a Service 
(SaaS) that doesn't need a database administrator. It allows users to focus on 
analyzing data to find meaningful insights using familiar SQL.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst


.. _howto/operator:BigQueryExecuteQueryOperator:

Executing queries
^^^^^^^^^^^^^^^^^

To execute BigQuery SQL queries in a specific BigQuery database you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator`.
