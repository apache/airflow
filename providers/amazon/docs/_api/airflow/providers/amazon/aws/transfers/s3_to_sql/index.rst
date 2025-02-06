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

:py:mod:`airflow.providers.amazon.aws.transfers.s3_to_sql`
==========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.s3_to_sql


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.s3_to_sql.S3ToSqlOperator




.. py:class:: S3ToSqlOperator(*, s3_key, s3_bucket, table, parser, column_list = None, commit_every = 1000, schema = None, sql_conn_id = 'sql_default', sql_hook_params = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Load Data from S3 into a SQL Database.

   You need to provide a parser function that takes a filename as an input
   and returns an iterable of rows

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3ToSqlOperator`

   :param schema: reference to a specific schema in SQL database
   :param table: reference to a specific table in SQL database
   :param s3_bucket: reference to a specific S3 bucket
   :param s3_key: reference to a specific S3 key
   :param sql_conn_id: reference to a specific SQL database. Must be of type DBApiHook
   :param sql_hook_params: Extra config params to be passed to the underlying hook.
       Should match the desired hook constructor params.
   :param aws_conn_id: reference to a specific S3 / AWS connection
   :param column_list: list of column names to use in the insert SQL.
   :param commit_every: The maximum number of rows to insert in one
       transaction. Set to `0` to insert all rows in one transaction.
   :param parser: parser function that takes a filepath as input and returns an iterable.
       e.g. to use a CSV parser that yields rows line-by-line, pass the following
       function:

       .. code-block:: python

           def parse_csv(filepath):
               import csv

               with open(filepath, newline="") as file:
                   yield from csv.reader(file)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('s3_bucket', 's3_key', 'schema', 'table', 'column_list', 'sql_conn_id')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#f4a460'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: db_hook()
