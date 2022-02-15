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

.. _howto/operator:RedshiftSQLOperator:

RedshiftSQLOperator
===================

.. contents::
  :depth: 1
  :local:

Overview
--------

Use the :class:`RedshiftSQLOperator <airflow.providers.amazon.aws.operators.redshift_sql>` to execute
statements against an Amazon Redshift cluster.

:class:`RedshiftSQLOperator <airflow.providers.amazon.aws.operators.redshift_sql.RedshiftSQLOperator>` works together with
:class:`RedshiftSQLHook <airflow.providers.amazon.aws.hooks.redshift.RedshiftSQLHook>` to establish
connections with Amazon Redshift.


example_redshift.py
-------------------

Purpose
"""""""

This is a basic example dag for using :class:`RedshiftSQLOperator <airflow.providers.amazon.aws.operators.redshift_sql>`
to execute statements against an Amazon Redshift cluster.

Create a table
""""""""""""""

In the following code we are creating a table called "fruit".

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_redshift.py
    :language: python
    :start-after: [START howto_operator_redshift_create_table]
    :end-before: [END howto_operator_redshift_create_table]

Insert data into a table
""""""""""""""""""""""""

In the following code we insert a few sample rows into the "fruit" table.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_redshift.py
    :language: python
    :start-after: [START howto_operator_redshift_populate_table]
    :end-before: [END howto_operator_redshift_populate_table]

Fetching records from a table
"""""""""""""""""""""""""""""

Creating a new table, "more_fruit" from the "fruit" table.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_redshift.py
    :language: python
    :start-after: [START howto_operator_redshift_get_all_rows]
    :end-before: [END howto_operator_redshift_get_all_rows]

Passing Parameters into RedshiftSQLOperator
"""""""""""""""""""""""""""""""""""""""""""

RedshiftSQLOperator supports the ``parameters`` attribute which allows us to dynamically pass
parameters into SQL statements.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_redshift.py
    :language: python
    :start-after: [START howto_operator_redshift_get_with_filter]
    :end-before: [END howto_operator_redshift_get_with_filter]

The complete RedshiftSQLOperator DAG
------------------------------------

All together, here is our DAG:

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_redshift.py
    :language: python
    :start-after: [START redshift_operator_howto_guide]
    :end-before: [END redshift_operator_howto_guide]
