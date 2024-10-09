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

===================
Amazon Athena SQL
===================

`Amazon Athena <https://aws.amazon.com/athena/>`__ is an interactive query service
that makes it easy to analyze data in Amazon Simple Storage Service (S3) using
standard SQL.  Athena is serverless, so there is no infrastructure to setup or
manage, and you pay only for the queries you run.  To get started, simply point
to your data in S3, define the schema, and start querying using standard SQL.

Prerequisite Tasks
------------------

.. include:: ../../_partials/prerequisite_tasks.rst

Operators
---------

Execute a SQL query
===================

The generic ``SQLExecuteQueryOperator`` can be used to execute SQL queries against Amazon Athena using a `Athena connection <../../connections/athena.html>`_.

To execute a single SQL query against an Amazon Athena without bringing back the results to Airflow,
please use ``AthenaOperator`` instead.

.. exampleinclude:: /../../providers/tests/system/common/sql/example_sql_execute_query.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_execute_query]
    :end-before: [END howto_operator_sql_execute_query]

Also, if you need to do simple data quality tests with Amazon Athena, you can use the ``SQLTableCheckOperator``

The below example demonstrates how to instantiate the SQLTableCheckOperator task.

.. exampleinclude:: /../../providers/tests/system/common/sql/example_sql_column_table_check.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_sql_table_check]
    :end-before: [END howto_operator_sql_table_check]

Reference
---------

* `PyAthena <https://github.com/laughingman7743/PyAthena>`__
