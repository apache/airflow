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



Google Cloud SQL Connection
===========================

The ``gcpcloudsql://`` connection is used by
:class:`airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExecuteQueryOperator` to perform query
on a Google Cloud SQL database. Google Cloud SQL database can be either
Postgres or MySQL, so this is a "meta" connection type. It introduces common schema
for both MySQL and Postgres, including what kind of connectivity should be used.
Google Cloud SQL supports connecting via public IP or via Cloud SQL Proxy.
In the latter case the
:class:`~airflow.providers.google.cloud.operators.hooks.cloud_sql.CloudSQLHook` uses
:class:`~airflow.providers.google.cloud.operators.hooks.CloudSqlProxyRunner` to automatically prepare
and use temporary Postgres or MySQL connection that will use the proxy to connect
(either via TCP or UNIX socket.

Configuring the Connection
--------------------------

Host (required)
    The host to connect to.

Schema (optional)
    Specify the schema name to be used in the database.

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Extra (optional)
    Specify the extra parameters (as JSON dictionary) that can be used in Google Cloud SQL
    connection.

    Details of all the parameters supported in extra field can be found in
    :class:`~airflow.providers.google.cloud.operators.hooks.cloud_sql.CloudSQLHook`.

    Example "extras" field:

    .. code-block:: json

       {
          "database_type": "mysql",
          "project_id": "example-project",
          "location": "europe-west1",
          "instance": "testinstance",
          "use_proxy": true,
          "sql_proxy_use_tcp": false
       }

    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable), you should specify
    it following the standard syntax of DB connection, where extras are passed as
    parameters of the URI. Note that all components of the URI should be URL-encoded.

    For example:

    .. code-block:: bash

        export AIRFLOW_CONN_GOOGLE_CLOUD_SQL_DEFAULT='gcpcloudsql://user:XXXXXXXXX@1.1.1.1:3306/mydb?database_type=mysql&project_id=example-project&location=europe-west1&instance=testinstance&use_proxy=True&sql_proxy_use_tcp=False'

Configuring and using IAM authentication
----------------------------------------

.. warning::
  This functionality requires ``gcloud`` command (Google Cloud SDK) must be `installed
  <https://cloud.google.com/sdk/docs/install>`_ on the Airflow worker.

.. warning::
  IAM authentication working only for Google Service Accounts.

Configure Service Accounts on Google Cloud IAM side
"""""""""""""""""""""""""""""""""""""""""""""""""""

For connecting via IAM you need to use Service Account. It can be the same service account which you use for
the ``gcloud`` authentication or an another account. If you decide to use a different account then this
account should be impersonated from the account which used for ``gcloud`` authentication and granted
a ``Service Account Token Creator`` role. More information how to grant a role `here
<https://cloud.google.com/iam/docs/manage-access-service-accounts?hl=en&_gl=1*3bsv5i*_ga*NDY4NDIyNTcxLjE3MjkxNzQ4MTM.*_ga_WH2QY8WWF5*MTcyOTE5MzU1OS4yLjEuMTcyOTE5NTM0My4wLjAuMA..#single-role>`_.

Also the Service Account should be configured for working with IAM.
Here are links describing what should be done before the start: `PostgreSQL
<https://cloud.google.com/sql/docs/postgres/iam-logins#before_you_begin>`_ and `MySQL
<https://cloud.google.com/sql/docs/mysql/iam-logins#before_you_begin>`_.

Configure ``gcpcloudsql`` connection with IAM enabling
""""""""""""""""""""""""""""""""""""""""""""""""""""""

For using IAM you need to enable ``"use_iam": "True"`` in the ``extra`` field. And specify IAM account in this format
``USERNAME@PROJECT_ID.iam.gserviceaccount.com`` in ``login`` field and empty string in the ``password`` field.

For example:

.. exampleinclude:: /../../providers/tests/system/google/cloud/cloud_sql/example_cloud_sql_query_iam.py
    :language: python
    :start-after: [START howto_operator_cloudsql_iam_connections]
    :end-before: [END howto_operator_cloudsql_iam_connections]
