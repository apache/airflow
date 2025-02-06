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

:py:mod:`airflow.providers.amazon.aws.transfers.redshift_to_s3`
===============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.redshift_to_s3

.. autoapi-nested-parse::

   Transfers data from AWS Redshift into a S3 Bucket.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.redshift_to_s3.RedshiftToS3Operator




.. py:class:: RedshiftToS3Operator(*, s3_bucket, s3_key, schema = None, table = None, select_query = None, redshift_conn_id = 'redshift_default', aws_conn_id = 'aws_default', verify = None, unload_options = None, autocommit = False, include_header = False, parameters = None, table_as_file_name = True, redshift_data_api_kwargs = {}, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Execute an UNLOAD command to s3 as a CSV with headers.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RedshiftToS3Operator`

   :param s3_bucket: reference to a specific S3 bucket
   :param s3_key: reference to a specific S3 key. If ``table_as_file_name`` is set
       to False, this param must include the desired file name
   :param schema: reference to a specific schema in redshift database
       Applicable when ``table`` param provided.
   :param table: reference to a specific table in redshift database
       Used when ``select_query`` param not provided.
   :param select_query: custom select query to fetch data from redshift database
   :param redshift_conn_id: reference to a specific redshift database
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
   :param unload_options: reference to a list of UNLOAD options
   :param autocommit: If set to True it will automatically commit the UNLOAD statement.
       Otherwise it will be committed right before the redshift connection gets closed.
   :param include_header: If set to True the s3 file contains the header columns.
   :param parameters: (optional) the parameters to render the SQL query with.
   :param table_as_file_name: If set to True, the s3 file will be named as the table.
       Applicable when ``table`` param provided.
   :param redshift_data_api_kwargs: If using the Redshift Data API instead of the SQL-based connection,
       dict of arguments for the hook's ``execute_query`` method.
       Cannot include any of these kwargs: ``{'sql', 'parameters'}``

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('s3_bucket', 's3_key', 'schema', 'table', 'unload_options', 'select_query', 'redshift_conn_id')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ('.sql',)



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: ui_color
      :value: '#ededed'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
