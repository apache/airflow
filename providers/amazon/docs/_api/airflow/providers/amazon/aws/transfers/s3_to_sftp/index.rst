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

:py:mod:`airflow.providers.amazon.aws.transfers.s3_to_sftp`
===========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.s3_to_sftp


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.s3_to_sftp.S3ToSFTPOperator




.. py:class:: S3ToSFTPOperator(*, s3_bucket, s3_key, sftp_path, sftp_conn_id = 'ssh_default', aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This operator enables the transferring of files from S3 to a SFTP server.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3ToSFTPOperator`

   :param sftp_conn_id: The sftp connection id. The name or identifier for
       establishing a connection to the SFTP server.
   :param sftp_path: The sftp remote path. This is the specified file path for
       uploading file to the SFTP server.
   :param aws_conn_id: aws connection to use
   :param s3_bucket: The targeted s3 bucket. This is the S3 bucket from
       where the file is downloaded.
   :param s3_key: The targeted s3 key. This is the specified file path for
       downloading the file from S3.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('s3_key', 'sftp_path', 's3_bucket')



   .. py:method:: get_s3_key(s3_key)
      :staticmethod:

      Parse the correct format for S3 keys regardless of how the S3 url is passed.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
