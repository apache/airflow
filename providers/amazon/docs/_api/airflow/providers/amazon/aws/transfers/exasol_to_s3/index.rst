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

:py:mod:`airflow.providers.amazon.aws.transfers.exasol_to_s3`
=============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.exasol_to_s3

.. autoapi-nested-parse::

   Transfers data from Exasol database into a S3 Bucket.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.exasol_to_s3.ExasolToS3Operator




.. py:class:: ExasolToS3Operator(*, query_or_table, key, bucket_name = None, replace = False, encrypt = False, gzip = False, acl_policy = None, query_params = None, export_params = None, exasol_conn_id = 'exasol_default', aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Export data from Exasol database to AWS S3 bucket.

   :param query_or_table: the sql statement to be executed or table name to export
   :param key: S3 key that will point to the file
   :param bucket_name: Name of the bucket in which to store the file
   :param replace: A flag to decide whether or not to overwrite the key
       if it already exists. If replace is False and the key exists, an
       error will be raised.
   :param encrypt: If True, the file will be encrypted on the server-side
       by S3 and will be stored in an encrypted form while at rest in S3.
   :param gzip: If True, the file will be compressed locally
   :param acl_policy: String specifying the canned ACL policy for the file being
       uploaded to the S3 bucket.
   :param query_params: Query parameters passed to underlying ``export_to_file``
       method of :class:`~pyexasol.connection.ExaConnection`.
   :param export_params: Extra parameters passed to underlying ``export_to_file``
       method of :class:`~pyexasol.connection.ExaConnection`.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('query_or_table', 'key', 'bucket_name', 'query_params', 'export_params')



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ('.sql',)



   .. py:attribute:: ui_color
      :value: '#ededed'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
