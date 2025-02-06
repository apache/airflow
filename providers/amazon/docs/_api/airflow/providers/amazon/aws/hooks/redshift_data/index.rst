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

:py:mod:`airflow.providers.amazon.aws.hooks.redshift_data`
==========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.redshift_data


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook




.. py:class:: RedshiftDataHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`\ [\ :py:obj:`mypy_boto3_redshift_data.RedshiftDataAPIServiceClient`\ ]

   Interact with Amazon Redshift Data API.

   Provide thin wrapper around
   :external+boto3:py:class:`boto3.client("redshift-data") <RedshiftDataAPIService.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
       - `Amazon Redshift Data API         <https://docs.aws.amazon.com/redshift-data/latest/APIReference/Welcome.html>`__

   .. py:method:: execute_query(database, sql, cluster_identifier = None, db_user = None, parameters = None, secret_arn = None, statement_name = None, with_event = False, wait_for_completion = True, poll_interval = 10, workgroup_name = None)

      Execute a statement against Amazon Redshift.

      :param database: the name of the database
      :param sql: the SQL statement or list of  SQL statement to run
      :param cluster_identifier: unique identifier of a cluster
      :param db_user: the database username
      :param parameters: the parameters for the SQL statement
      :param secret_arn: the name or ARN of the secret that enables db access
      :param statement_name: the name of the SQL statement
      :param with_event: indicates whether to send an event to EventBridge
      :param wait_for_completion: indicates whether to wait for a result, if True wait, if False don't wait
      :param poll_interval: how often in seconds to check the query status
      :param workgroup_name: name of the Redshift Serverless workgroup. Mutually exclusive with
          `cluster_identifier`. Specify this parameter to query Redshift Serverless. More info
          https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html

      :returns statement_id: str, the UUID of the statement


   .. py:method:: wait_for_results(statement_id, poll_interval)


   .. py:method:: get_table_primary_key(table, database, schema = 'public', cluster_identifier = None, workgroup_name = None, db_user = None, secret_arn = None, statement_name = None, with_event = False, wait_for_completion = True, poll_interval = 10)

      Return the table primary key.

      Copied from ``RedshiftSQLHook.get_table_primary_key()``

      :param table: Name of the target table
      :param database: the name of the database
      :param schema: Name of the target schema, public by default
      :param sql: the SQL statement or list of  SQL statement to run
      :param cluster_identifier: unique identifier of a cluster
      :param db_user: the database username
      :param secret_arn: the name or ARN of the secret that enables db access
      :param statement_name: the name of the SQL statement
      :param with_event: indicates whether to send an event to EventBridge
      :param wait_for_completion: indicates whether to wait for a result, if True wait, if False don't wait
      :param poll_interval: how often in seconds to check the query status

      :return: Primary key columns list
