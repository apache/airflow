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

:py:mod:`airflow.providers.amazon.aws.operators.redshift_data`
==============================================================

.. py:module:: airflow.providers.amazon.aws.operators.redshift_data


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator




.. py:class:: RedshiftDataOperator(database, sql, cluster_identifier = None, db_user = None, parameters = None, secret_arn = None, statement_name = None, with_event = False, wait_for_completion = True, poll_interval = 10, return_sql_result = False, aws_conn_id = 'aws_default', region = None, workgroup_name = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Executes SQL Statements against an Amazon Redshift cluster using Redshift Data.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RedshiftDataOperator`

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
   :param return_sql_result: if True will return the result of an SQL statement,
       if False (default) will return statement ID
   :param aws_conn_id: aws connection to use
   :param region: aws region to use
   :param workgroup_name: name of the Redshift Serverless workgroup. Mutually exclusive with
       `cluster_identifier`. Specify this parameter to query Redshift Serverless. More info
       https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-serverless.html

   .. py:attribute:: template_fields
      :value: ('cluster_identifier', 'database', 'sql', 'db_user', 'parameters', 'statement_name',...



   .. py:attribute:: template_ext
      :value: ('.sql',)



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: statement_id
      :type: str | None



   .. py:method:: hook()

      Create and return an RedshiftDataHook.


   .. py:method:: execute(context)

      Execute a statement against Amazon Redshift.


   .. py:method:: on_kill()

      Cancel the submitted redshift query.
