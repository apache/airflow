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

======================
AWS Glue Data Catalog
======================

The AWS Glue Data Catalog is a centralized metadata repository for data assets.
Use the operators below to manage Glue Data Catalog resources.

.. _howto/operator:GlueCatalogCreateDatabaseOperator:

Create a Catalog Database
~~~~~~~~~~~~~~~~~~~~~~~~~

To create a database in the AWS Glue Data Catalog, use
:class:`~airflow.providers.amazon.aws.operators.glue_catalog.GlueCatalogCreateDatabaseOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_catalog_create_database]
    :end-before: [END howto_operator_glue_catalog_create_database]

Reference
~~~~~~~~~

* `AWS boto3 Library Documentation for Glue <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html>`__

.. _howto/operator:GlueCatalogCreateTableOperator:

Create a Table
--------------

To create a table in an AWS Glue Data Catalog database, use
:class:`~airflow.providers.amazon.aws.operators.glue_catalog.GlueCatalogCreateTableOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_catalog_create_table]
    :end-before: [END howto_operator_glue_catalog_create_table]

.. _howto/operator:GlueCatalogDeleteDatabaseOperator:

Delete a Catalog Database
~~~~~~~~~~~~~~~~~~~~~~~~~

To delete a database from the AWS Glue Data Catalog, use
:class:`~airflow.providers.amazon.aws.operators.glue_catalog.GlueCatalogDeleteDatabaseOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_catalog_delete_database]
    :end-before: [END howto_operator_glue_catalog_delete_database]

.. _howto/operator:GlueCatalogDeleteTableOperator:

Delete a Table
--------------

To delete a table from an AWS Glue Data Catalog database, use
:class:`~airflow.providers.amazon.aws.operators.glue_catalog.GlueCatalogDeleteTableOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_glue_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_glue_catalog_delete_table]
    :end-before: [END howto_operator_glue_catalog_delete_table]
