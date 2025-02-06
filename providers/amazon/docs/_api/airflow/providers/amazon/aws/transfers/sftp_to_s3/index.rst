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

:py:mod:`airflow.providers.amazon.aws.transfers.sftp_to_s3`
===========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.sftp_to_s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.sftp_to_s3.SFTPToS3Operator




.. py:class:: SFTPToS3Operator(*, s3_bucket, s3_key, sftp_path, sftp_conn_id = 'ssh_default', s3_conn_id = 'aws_default', use_temp_file = True, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Transfer files from an SFTP server to Amazon S3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SFTPToS3Operator`

   :param sftp_conn_id: The sftp connection id. The name or identifier for
       establishing a connection to the SFTP server.
   :param sftp_path: The sftp remote path. This is the specified file path
       for downloading the file from the SFTP server.
   :param s3_conn_id: The s3 connection id. The name or identifier for
       establishing a connection to S3
   :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
       the file is uploaded.
   :param s3_key: The targeted s3 key. This is the specified path for
       uploading the file to S3.
   :param use_temp_file: If True, copies file first to local,
       if False streams file from SFTP to S3.

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
