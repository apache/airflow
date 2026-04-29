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

==================
Amazon S3 Tables
==================

.. _howto/operator:S3TablesCreateTableOperator:

Create an Amazon S3 Table
=========================

To create a new Iceberg table in an Amazon S3 Tables namespace you can use
:class:`~airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3tables_create_table]
    :end-before: [END howto_operator_s3tables_create_table]

.. _howto/operator:S3TablesDeleteTableOperator:

Delete a Table
~~~~~~~~~~~~~~

To delete a table from an Amazon S3 Tables namespace, use
:class:`~airflow.providers.amazon.aws.operators.s3_tables.S3TablesDeleteTableOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3tables_delete_table]
    :end-before: [END howto_operator_s3tables_delete_table]
