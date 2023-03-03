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



Google Cloud SQL Operators
==========================

Prerequisite Tasks
------------------

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:CloudSQLCreateInstanceDatabaseOperator:

CloudSQLCreateInstanceDatabaseOperator
--------------------------------------

Creates a new database inside a Cloud SQL instance.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCreateInstanceDatabaseOperator`.


Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_create]
    :end-before: [END howto_operator_cloudsql_db_create]

Example request body:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_db_create_body]
    :end-before: [END howto_operator_cloudsql_db_create_body]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_db_create_template_fields]
    :end-before: [END gcp_sql_db_create_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation for `to create a new database inside the instance
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/insert>`_.

.. _howto/operator:CloudSQLDeleteInstanceDatabaseOperator:

CloudSQLDeleteInstanceDatabaseOperator
--------------------------------------

Deletes a database from a Cloud SQL instance.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceDatabaseOperator`.


Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_delete]
    :end-before: [END howto_operator_cloudsql_db_delete]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_db_delete_template_fields]
    :end-before: [END gcp_sql_db_delete_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation to `delete a database
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/delete>`_.

.. _howto/operator:CloudSQLPatchInstanceDatabaseOperator:

CloudSQLPatchInstanceDatabaseOperator
-------------------------------------

Updates a resource containing information about a database inside a Cloud SQL instance
using patch semantics.
See: https://cloud.google.com/sql/docs/mysql/admin-api/how-tos/performance#patch

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLPatchInstanceDatabaseOperator`.


Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_db_patch]
    :end-before: [END howto_operator_cloudsql_db_patch]

Example request body:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_db_patch_body]
    :end-before: [END howto_operator_cloudsql_db_patch_body]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_db_patch_template_fields]
    :end-before: [END gcp_sql_db_patch_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation to `update a database
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/databases/patch>`_.

.. _howto/operator:CloudSQLDeleteInstanceOperator:

CloudSQLDeleteInstanceOperator
------------------------------

Deletes a Cloud SQL instance in Google Cloud.

It is also used for deleting read and failover replicas.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceOperator`.


Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_delete]
    :end-before: [END howto_operator_cloudsql_delete]

Note: If the instance has read or failover replicas you need to delete them before you delete the primary instance.
Replicas are deleted the same way as primary instances:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_replicas_delete]
    :end-before: [END howto_operator_cloudsql_replicas_delete]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_delete_template_fields]
    :end-before: [END gcp_sql_delete_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation to `delete a SQL instance
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/delete>`_.

.. _howto/operator:CloudSQLExportInstanceOperator:

CloudSQLExportInstanceOperator
------------------------------

Exports data from a Cloud SQL instance to a Cloud Storage bucket as a SQL dump
or CSV file.

.. note::
    This operator is idempotent. If executed multiple times with the same
    export file URI, the export file in GCS will simply be overridden.

For parameter definition take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExportInstanceOperator`.

Arguments
"""""""""

Example body defining the export operation:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_export_body]
    :end-before: [END howto_operator_cloudsql_export_body]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_export]
    :end-before: [END howto_operator_cloudsql_export]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_export_template_fields]
    :end-before: [END gcp_sql_export_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation to `export data
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export>`_.

Troubleshooting
"""""""""""""""

If you receive an "Unauthorized" error in Google Cloud, make sure that the service account
of the Cloud SQL instance is authorized to write to the selected GCS bucket.

It is not the service account configured in Airflow that communicates with GCS,
but rather the service account of the particular Cloud SQL instance.

To grant the service account with the appropriate WRITE permissions for the GCS bucket
you can use the :class:`~airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator`,
as shown in the example:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_export_gcs_permissions]
    :end-before: [END howto_operator_cloudsql_export_gcs_permissions]


.. _howto/operator:CloudSQLImportInstanceOperator:

CloudSQLImportInstanceOperator
------------------------------

Imports data into a Cloud SQL instance from a SQL dump or CSV file in Cloud Storage.

CSV import:
"""""""""""

This operator is NOT idempotent for a CSV import. If the same file is imported
multiple times, the imported data will be duplicated in the database.
Moreover, if there are any unique constraints the duplicate import may result in an
error.

SQL import:
"""""""""""

This operator is idempotent for a SQL import if it was also exported by Cloud SQL.
The exported SQL contains 'DROP TABLE IF EXISTS' statements for all tables
to be imported.

If the import file was generated in a different way, idempotence is not guaranteed.
It has to be ensured on the SQL file level.

For parameter definition take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLImportInstanceOperator`.

Arguments
"""""""""

Example body defining the import operation:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_import_body]
    :end-before: [END howto_operator_cloudsql_import_body]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_import]
    :end-before: [END howto_operator_cloudsql_import]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_import_template_fields]
    :end-before: [END gcp_sql_import_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation to `import data
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/import>`_.

Troubleshooting
"""""""""""""""

If you receive an "Unauthorized" error in Google Cloud, make sure that the service account
of the Cloud SQL instance is authorized to read from the selected GCS object.

It is not the service account configured in Airflow that communicates with GCS,
but rather the service account of the particular Cloud SQL instance.

To grant the service account with the appropriate READ permissions for the GCS object
you can use the :class:`~airflow.providers.google.cloud.operators.gcs.GCSBucketCreateAclEntryOperator`,
as shown in the example:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_import_gcs_permissions]
    :end-before: [END howto_operator_cloudsql_import_gcs_permissions]

.. _howto/operator:CloudSQLCreateInstanceOperator:

CloudSQLCreateInstanceOperator
------------------------------

Creates a new Cloud SQL instance in Google Cloud.

It is also used for creating read replicas.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCreateInstanceOperator`.

If an instance with the same name exists, no action will be taken and the operator
will succeed.

Arguments
"""""""""

Example body defining the instance with failover replica:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_create_body]
    :end-before: [END howto_operator_cloudsql_create_body]

Example body defining read replica for the instance above:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_create_replica]
    :end-before: [END howto_operator_cloudsql_create_replica]

Note: Failover replicas are created together with the instance in a single task.
Read replicas need to be created in separate tasks.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_create]
    :end-before: [END howto_operator_cloudsql_create]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_create_template_fields]
    :end-before: [END gcp_sql_create_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation to `create an instance
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/insert>`_.

.. _howto/operator:CloudSQLInstancePatchOperator:

CloudSQLInstancePatchOperator
-----------------------------

Updates settings of a Cloud SQL instance in Google Cloud (partial update).

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLInstancePatchOperator`.

This is a partial update, so only values for the settings specified in the body
will be set / updated. The rest of the existing instance's configuration will remain
unchanged.

Arguments
"""""""""

Example body defining the instance:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :start-after: [START howto_operator_cloudsql_patch_body]
    :end-before: [END howto_operator_cloudsql_patch_body]

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the Google Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_patch]
    :end-before: [END howto_operator_cloudsql_patch]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_patch_template_fields]
    :end-before: [END gcp_sql_patch_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation to `patch an instance
<https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/patch>`_.

.. _howto/operator:CloudSQLCloneInstanceOperator:

CloudSQLCloneInstanceOperator
-----------------------------

Clones an Cloud SQL instance.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLCloneInstanceOperator`.

Arguments
"""""""""

For ``clone_context`` object attributes please refer to
`CloneContext <https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/instances/clone#clonecontext>`_

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing it will be retrieved from the Google
Cloud connection used. Both variants are shown:

.. exampleinclude:: /../../tests/system/providers/google/cloud/cloud_sql/example_cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudsql_clone]
    :end-before: [END howto_operator_cloudsql_clone]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_clone_template_fields]
    :end-before: [END gcp_sql_clone_template_fields]

More information
""""""""""""""""

See Google Cloud SQL API documentation to `clone an instance
<https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/instances/clone>`_.

.. _howto/operator:CloudSQLExecuteQueryOperator:

CloudSQLExecuteQueryOperator
----------------------------

Performs DDL or DML SQL queries in Google Cloud SQL instance. The DQL
(retrieving data from Google Cloud SQL) is not supported. You might run the SELECT
queries, but the results of those queries are discarded.

You can specify various connectivity methods to connect to running instance,
starting from public IP plain connection through public IP with SSL or both TCP and
socket connection via Cloud SQL Proxy. The proxy is downloaded and started/stopped
dynamically as needed by the operator.

There is a ``gcpcloudsql://*`` connection type that you should use to define what
kind of connectivity you want the operator to use. The connection is a "meta"
type of connection. It is not used to make an actual connectivity on its own, but it
determines whether Cloud SQL Proxy should be started by ``CloudSQLDatabaseHook``
and what kind of database connection (Postgres or MySQL) should be created
dynamically to connect to Cloud SQL via public IP address or via the proxy.
The ``CloudSqlDatabaseHook`` uses
:class:`~airflow.providers.google.cloud.hooks.cloud_sql.CloudSqlProxyRunner` to manage Cloud SQL
Proxy lifecycle (each task has its own Cloud SQL Proxy)

When you build connection, you should use connection parameters as described in
:class:`~airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook`. You can see
examples of connections below for all the possible types of connectivity. Such connection
can be reused between different tasks (instances of ``CloudSqlQueryOperator``). Each
task will get their own proxy started if needed with their own TCP or UNIX socket.

For parameter definition, take a look at
:class:`~airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExecuteQueryOperator`.

Since query operator can run arbitrary query, it cannot be guaranteed to be
idempotent. SQL query designer should design the queries to be idempotent. For example,
both Postgres and MySQL support CREATE TABLE IF NOT EXISTS statements that can be
used to create tables in an idempotent way.

Arguments
"""""""""

If you define connection via :envvar:`AIRFLOW_CONN_{CONN_ID}` URL defined in an environment
variable, make sure the URL components in the URL are URL-encoded.
See examples below for details.

Note that in case of SSL connections you need to have a mechanism to make the
certificate/key files available in predefined locations for all the workers on
which the operator can run. This can be provided for example by mounting
NFS-like volumes in the same path for all the workers.

Example connection definitions for all connectivity cases. Note that all the components
of the connection URI should be URL-encoded:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_sql_query.py
    :language: python
    :start-after: [START howto_operator_cloudsql_query_connections]
    :end-before: [END howto_operator_cloudsql_query_connections]

Using the operator
""""""""""""""""""

Example operators below are using all connectivity options. Note connection id
from the operator matches the :envvar:`AIRFLOW_CONN_{CONN_ID}` postfix uppercase. This is
standard AIRFLOW notation for defining connection via environment variables):

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_cloud_sql_query.py
    :language: python
    :start-after: [START howto_operator_cloudsql_query_operators]
    :end-before: [END howto_operator_cloudsql_query_operators]

Templating
""""""""""

.. literalinclude:: /../../airflow/providers/google/cloud/operators/cloud_sql.py
    :language: python
    :dedent: 4
    :start-after: [START gcp_sql_query_template_fields]
    :end-before: [END gcp_sql_query_template_fields]

More information
""""""""""""""""

See Google Cloud SQL documentation for `MySQL <https://cloud.google.com/sql/docs/mysql/sql-proxy>`_ and
`PostgreSQL <https://cloud.google.com/sql/docs/postgres/sql-proxy>`_ related proxies.

Reference
---------

For further information, look at:

* `Google Cloud API - MySQL Documentation <https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/>`__
* `Google Cloud API - PostgreSQL Documentation <https://cloud.google.com/sql/docs/postgres/admin-api/v1beta4/>`__
* `Product Documentation <https://cloud.google.com/sql/docs/>`__
