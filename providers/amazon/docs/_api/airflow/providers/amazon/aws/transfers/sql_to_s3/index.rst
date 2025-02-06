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

:py:mod:`airflow.providers.amazon.aws.transfers.sql_to_s3`
==========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.sql_to_s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.sql_to_s3.FILE_FORMAT
   airflow.providers.amazon.aws.transfers.sql_to_s3.SqlToS3Operator




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.sql_to_s3.FileOptions
   airflow.providers.amazon.aws.transfers.sql_to_s3.FILE_OPTIONS_MAP


.. py:class:: FILE_FORMAT


   Bases: :py:obj:`enum.Enum`

   Possible file formats.

   .. py:attribute:: CSV



   .. py:attribute:: JSON



   .. py:attribute:: PARQUET




.. py:data:: FileOptions



.. py:data:: FILE_OPTIONS_MAP



.. py:class:: SqlToS3Operator(*, query, s3_bucket, s3_key, sql_conn_id, sql_hook_params = None, parameters = None, replace = False, aws_conn_id = 'aws_default', verify = None, file_format = 'csv', pd_kwargs = None, groupby_kwargs = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Saves data from a specific SQL query into a file in S3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SqlToS3Operator`

   :param query: the sql query to be executed. If you want to execute a file, place the absolute path of it,
       ending with .sql extension. (templated)
   :param s3_bucket: bucket where the data will be stored. (templated)
   :param s3_key: desired key for the file. It includes the name of the file. (templated)
   :param replace: whether or not to replace the file in S3 if it previously existed
   :param sql_conn_id: reference to a specific database.
   :param sql_hook_params: Extra config params to be passed to the underlying hook.
       Should match the desired hook constructor params.
   :param parameters: (optional) the parameters to render the SQL query with.
   :param aws_conn_id: reference to a specific S3 connection
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
               (unless use_ssl is False), but SSL certificates will not be verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
               You can specify this argument if you want to use a different
               CA cert bundle than the one used by botocore.
   :param file_format: the destination file format, only string 'csv', 'json' or 'parquet' is accepted.
   :param pd_kwargs: arguments to include in DataFrame ``.to_parquet()``, ``.to_json()`` or ``.to_csv()``.
   :param groupby_kwargs: argument to include in DataFrame ``groupby()``.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('s3_bucket', 's3_key', 'query', 'sql_conn_id')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ('.sql',)



   .. py:attribute:: template_fields_renderers



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
