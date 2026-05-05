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

.. _howto/operator:S3TablesCreateTableBucketOperator:

Create a Table Bucket
---------------------

To create an Amazon S3 Tables table bucket, use
:class:`~airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableBucketOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3tables_create_table_bucket]
    :end-before: [END howto_operator_s3tables_create_table_bucket]

.. _howto/operator:S3TablesCreateNamespaceOperator:

Create a Namespace
------------------

To create a namespace in an Amazon S3 Tables table bucket, use
:class:`~airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateNamespaceOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3tables_create_namespace]
    :end-before: [END howto_operator_s3tables_create_namespace]

.. _howto/operator:S3TablesCreateTableOperator:

Create a Table
--------------

To create a new Iceberg table in an Amazon S3 Tables namespace you can use
:class:`~airflow.providers.amazon.aws.operators.s3_tables.S3TablesCreateTableOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3tables_create_table]
    :end-before: [END howto_operator_s3tables_create_table]

.. _howto/operator:S3TablesDeleteNamespaceOperator:

Delete a Namespace
------------------

To delete a namespace from an Amazon S3 Tables table bucket, use
:class:`~airflow.providers.amazon.aws.operators.s3_tables.S3TablesDeleteNamespaceOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3tables_delete_namespace]
    :end-before: [END howto_operator_s3tables_delete_namespace]

.. _howto/operator:S3TablesDeleteTableOperator:

Delete a Table
--------------

To delete a table from an Amazon S3 Tables namespace, use
:class:`~airflow.providers.amazon.aws.operators.s3_tables.S3TablesDeleteTableOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3tables_delete_table]
    :end-before: [END howto_operator_s3tables_delete_table]

.. _howto/operator:S3TablesDeleteTableBucketOperator:

Delete a Table Bucket
---------------------

To delete an Amazon S3 Tables table bucket, use
:class:`~airflow.providers.amazon.aws.operators.s3_tables.S3TablesDeleteTableBucketOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_s3_tables.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_s3tables_delete_table_bucket]
    :end-before: [END howto_operator_s3tables_delete_table_bucket]

Reference
---------

* `AWS boto3 Library Documentation for S3 Tables <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3tables.html>`__
