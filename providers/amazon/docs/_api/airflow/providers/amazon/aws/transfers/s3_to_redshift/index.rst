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

:py:mod:`airflow.providers.amazon.aws.transfers.s3_to_redshift`
===============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.s3_to_redshift


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.s3_to_redshift.S3ToRedshiftOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.s3_to_redshift.AVAILABLE_METHODS


.. py:data:: AVAILABLE_METHODS
   :value: ['APPEND', 'REPLACE', 'UPSERT']



.. py:class:: S3ToRedshiftOperator(*, schema, table, s3_bucket, s3_key, redshift_conn_id = 'redshift_default', aws_conn_id = 'aws_default', verify = None, column_list = None, copy_options = None, autocommit = False, method = 'APPEND', upsert_keys = None, redshift_data_api_kwargs = {}, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Executes an COPY command to load files from s3 to Redshift.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3ToRedshiftOperator`

   :param schema: reference to a specific schema in redshift database
   :param table: reference to a specific table in redshift database
   :param s3_bucket: reference to a specific S3 bucket
   :param s3_key: key prefix that selects single or multiple objects from S3
   :param redshift_conn_id: reference to a specific redshift database OR a redshift data-api connection
   :param aws_conn_id: reference to a specific S3 connection
       If the AWS connection contains 'aws_iam_role' in ``extras``
       the operator will use AWS STS credentials with a token
       https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html#copy-credentials
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :param column_list: list of column names to load
   :param copy_options: reference to a list of COPY options
   :param method: Action to be performed on execution. Available ``APPEND``, ``UPSERT`` and ``REPLACE``.
   :param upsert_keys: List of fields to use as key on upsert action
   :param redshift_data_api_kwargs: If using the Redshift Data API instead of the SQL-based connection,
       dict of arguments for the hook's ``execute_query`` method.
       Cannot include any of these kwargs: ``{'sql', 'parameters'}``

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('s3_bucket', 's3_key', 'schema', 'table', 'column_list', 'copy_options', 'redshift_conn_id', 'method')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#99e699'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
