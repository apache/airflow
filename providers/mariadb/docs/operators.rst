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



.. _howto/operator:mariadb:

How-to Guide for MariaDB using MariaDBOperator
==============================================

Use the :class:`~airflow.providers.mariadb.operators.mariadb.MariaDBOperator` to execute
SQL commands in a `MariaDB <https://mariadb.org/>`__ database.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your MariaDB instance where
the connection metadata is structured as follows:

.. list-table:: MariaDB Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - MariaDB hostname
   * - Schema: string
     - Set schema to execute SQL operations on by default
   * - Login: string
     - MariaDB user
   * - Password: string
     - MariaDB user password
   * - Port: int
     - MariaDB port

An example usage of the MariaDBOperator is as follows:

.. exampleinclude:: /../../mariadb/docs/example_dags/example_mariadb_basic.py
    :language: python
    :start-after: [START howto_operator_mariadb]
    :end-before: [END howto_operator_mariadb]

.. _howto/operator:mariadb_cpimport:

How-to Guide for MariaDB ColumnStore using CpimportOperator
===========================================================

Use the :class:`~airflow.providers.mariadb.operators.cpimport.CpimportOperator` to perform
bulk data loading operations in MariaDB ColumnStore using the cpimport utility.

Using the CpimportOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^

The CpimportOperator allows you to load data from CSV files into MariaDB ColumnStore tables
with high performance. This is particularly useful for large data sets.

An example usage of the CpimportOperator is as follows:

.. exampleinclude:: /../../mariadb/docs/example_dags/example_mariadb_cpimport.py
    :language: python
    :start-after: [START howto_operator_cpimport]
    :end-before: [END howto_operator_cpimport]

.. _howto/operator:mariadb_s3:

How-to Guide for MariaDB S3 Integration using S3Operator
=========================================================

Use the :class:`~airflow.providers.mariadb.operators.s3.S3Operator` to transfer data
between MariaDB and Amazon S3.

Using the S3Operator
^^^^^^^^^^^^^^^^^^^^

The S3Operator provides functionality to upload data from MariaDB to S3 or download
data from S3 to MariaDB, enabling cloud-based data workflows.

An example usage of the S3Operator is as follows:

.. exampleinclude:: /../../mariadb/docs/example_dags/example_mariadb_s3.py
    :language: python
    :start-after: [START howto_operator_s3]
    :end-before: [END howto_operator_s3]

.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``login``, ``password`` and so forth).
