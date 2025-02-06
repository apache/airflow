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

:py:mod:`airflow.providers.amazon.aws.transfers.hive_to_dynamodb`
=================================================================

.. py:module:: airflow.providers.amazon.aws.transfers.hive_to_dynamodb

.. autoapi-nested-parse::

   This module contains operator to move data from Hive to DynamoDB.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.hive_to_dynamodb.HiveToDynamoDBOperator




.. py:class:: HiveToDynamoDBOperator(*, sql, table_name, table_keys, pre_process = None, pre_process_args = None, pre_process_kwargs = None, region_name = None, schema = 'default', hiveserver2_conn_id = 'hiveserver2_default', aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Moves data from Hive to DynamoDB.

   Note that for now the data is loaded into memory before being pushed
   to DynamoDB, so this operator should be used for smallish amount of data.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/transfer:HiveToDynamoDBOperator`

   :param sql: SQL query to execute against the hive database. (templated)
   :param table_name: target DynamoDB table
   :param table_keys: partition key and sort key
   :param pre_process: implement pre-processing of source data
   :param pre_process_args: list of pre_process function arguments
   :param pre_process_kwargs: dict of pre_process function arguments
   :param region_name: aws region name (example: us-east-1)
   :param schema: hive database schema
   :param hiveserver2_conn_id: Reference to the
       :ref: `Hive Server2 thrift service connection id <howto/connection:hiveserver2>`.
   :param aws_conn_id: aws connection

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('sql',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ('.sql',)



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: ui_color
      :value: '#a0e08c'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
