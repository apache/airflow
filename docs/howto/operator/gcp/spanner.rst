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



Google Cloud Spanner Operators
==============================

.. contents::
  :depth: 1
  :local:

.. _howto/operator:CloudSpannerInstanceDatabaseDeleteOperator:

CloudSpannerInstanceDatabaseDeleteOperator
------------------------------------------

Deletes a database from the specified Cloud Spanner instance. If the database does not
exist, no action is taken, and the operator succeeds.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_database_delete]
    :end-before: [END howto_operator_spanner_database_delete]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_delete_template_fields]
    :end-before: [END gcp_spanner_delete_template_fields]

More information
""""""""""""""""

See `Google Cloud Spanner API documentation to drop an instance of a database
<https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/dropDatabase>`_.

.. _howto/operator:CloudSpannerInstanceDatabaseDeployOperator:

CloudSpannerInstanceDatabaseDeployOperator
------------------------------------------

Creates a new Cloud Spanner database in the specified instance, or if the
desired database exists, assumes success with no changes applied to database
configuration. No structure of the database is verified - it's enough if the database exists
with the same name.

For parameter definition, take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseDeployOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_database_deploy]
    :end-before: [END howto_operator_spanner_database_deploy]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_database_deploy_template_fields]
    :end-before: [END gcp_spanner_database_deploy_template_fields]

More information
""""""""""""""""

See Google Cloud Spanner API documentation `to create a new database
<https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/create>`_.

.. _howto/operator:CloudSpannerInstanceDatabaseUpdateOperator:

CloudSpannerInstanceDatabaseUpdateOperator
------------------------------------------

Runs a DDL query in a Cloud Spanner database and allows you to modify the structure of an
existing database.

You can optionally specify an operation_id parameter which simplifies determining whether
the statements were executed in case the update_database call is replayed
(idempotency check). The operation_id should be unique within the database, and must be
a valid identifier: `[a-z][a-z0-9_]*`. More information can be found in
`the documentation of updateDdl API <https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/updateDdl>`_

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseUpdateOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_database_update]
    :end-before: [END howto_operator_spanner_database_update]

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_database_update_idempotent]
    :end-before: [END howto_operator_spanner_database_update_idempotent]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_database_update_template_fields]
    :end-before: [END gcp_spanner_database_update_template_fields]

More information
""""""""""""""""

See Google Cloud Spanner API documentation for `database update_ddl
<https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/updateDdl>`_.

.. _howto/operator:CloudSpannerInstanceDatabaseQueryOperator:

CloudSpannerInstanceDatabaseQueryOperator
-----------------------------------------

Executes an arbitrary DML query (INSERT, UPDATE, DELETE).

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDatabaseQueryOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables.

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_query]
    :end-before: [END howto_operator_spanner_query]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_query_template_fields]
    :end-before: [END gcp_spanner_query_template_fields]

More information
""""""""""""""""

See Google Cloud Spanner API documentation for more information about `DML syntax
<https://cloud.google.com/spanner/docs/dml-syntax>`_.

.. _howto/operator:CloudSpannerInstanceDeleteOperator:

CloudSpannerInstanceDeleteOperator
----------------------------------

Deletes a Cloud Spanner instance. If an instance does not exist, no action is taken,
and the operator succeeds.

For parameter definition take a look at
:class:`~airflow.contrib.operators.gcp_spanner_operator.CloudSpannerInstanceDeleteOperator`.

Arguments
"""""""""

Some arguments in the example DAG are taken from environment variables:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :start-after: [START howto_operator_spanner_arguments]
    :end-before: [END howto_operator_spanner_arguments]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/contrib/example_dags/example_gcp_spanner.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_spanner_delete]
    :end-before: [END howto_operator_spanner_delete]

Templating
""""""""""

.. literalinclude:: ../../../../airflow/contrib/operators/gcp_spanner_operator.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_spanner_delete_template_fields]
    :end-before: [END gcp_spanner_delete_template_fields]

More information
""""""""""""""""

See Google Cloud Spanner API documentation to `delete an instance
<https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances/delete>`_.
