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

:py:mod:`airflow.providers.amazon.aws.hooks.redshift_sql`
=========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.redshift_sql


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook




.. py:class:: RedshiftSQLHook(*args, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.providers.common.sql.hooks.sql.DbApiHook`

   Execute statements against Amazon Redshift.

   This hook requires the redshift_conn_id connection.

   Note: For AWS IAM authentication, use iam in the extra connection parameters
   and set it to true. Leave the password field empty. This will use the
   "aws_default" connection to get the temporary token unless you override
   with aws_conn_id when initializing the hook.
   The cluster-identifier is extracted from the beginning of
   the host field, so is optional. It can however be overridden in the extra field.
   extras example: ``{"iam":true}``

   :param redshift_conn_id: reference to
       :ref:`Amazon Redshift connection id<howto/connection:redshift>`

   .. note::
       get_sqlalchemy_engine() and get_uri() depend on sqlalchemy-amazon-redshift

   .. py:attribute:: conn_name_attr
      :value: 'redshift_conn_id'



   .. py:attribute:: default_conn_name
      :value: 'redshift_default'



   .. py:attribute:: conn_type
      :value: 'redshift'



   .. py:attribute:: hook_name
      :value: 'Amazon Redshift'



   .. py:attribute:: supports_autocommit
      :value: True



   .. py:method:: get_ui_field_behaviour()
      :staticmethod:

      Get custom field behavior.


   .. py:method:: conn()


   .. py:method:: get_iam_token(conn)

      Retrieve a temporary password to connect to Redshift.

      Port is required. If none is provided, default is used for each service.


   .. py:method:: get_uri()

      Overridden to use the Redshift dialect as driver name.


   .. py:method:: get_sqlalchemy_engine(engine_kwargs=None)

      Overridden to pass Redshift-specific arguments.


   .. py:method:: get_table_primary_key(table, schema = 'public')

      Get the table's primary key.

      :param table: Name of the target table
      :param schema: Name of the target schema, public by default
      :return: Primary key columns list


   .. py:method:: get_conn()

      Get a ``redshift_connector.Connection`` object.
