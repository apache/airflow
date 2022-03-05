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

.. _howto/operator:RedshiftDataOperator:

RedshiftDataOperator
====================

.. contents::
  :depth: 1
  :local:

Overview
--------

Use the :class:`RedshiftDataOperator <airflow.providers.amazon.aws.operators.redshift_data>` to execute
statements against an Amazon Redshift cluster.

This differs from RedshiftSQLOperator in that it allows users to query and retrieve data via the AWS API and avoid the necessity of a Postgres connection.

example_redshift_data_execute_sql.py
------------------------------------

Purpose
"""""""

This is a basic example DAG for using :class:`RedshiftDataOperator <airflow.providers.amazon.aws.operators.redshift_data>`
to execute statements against an Amazon Redshift cluster.

List tables in database
"""""""""""""""""""""""

In the following code we list the tables in the provided database.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_redshift_data_execute_sql.py
    :language: python
    :start-after: [START howto_redshift_data]
    :end-before: [END howto_redshift_data]
