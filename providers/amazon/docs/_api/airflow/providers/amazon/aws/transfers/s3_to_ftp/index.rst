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

:py:mod:`airflow.providers.amazon.aws.transfers.s3_to_ftp`
==========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.s3_to_ftp


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.s3_to_ftp.S3ToFTPOperator




.. py:class:: S3ToFTPOperator(*, s3_bucket, s3_key, ftp_path, aws_conn_id='aws_default', ftp_conn_id='ftp_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This operator enables the transferring of files from S3 to a FTP server.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:S3ToFTPOperator`

   :param s3_bucket: The targeted s3 bucket. This is the S3 bucket from
       where the file is downloaded.
   :param s3_key: The targeted s3 key. This is the specified file path for
       downloading the file from S3.
   :param ftp_path: The ftp remote path. This is the specified file path for
       uploading file to the FTP server.
   :param aws_conn_id: reference to a specific AWS connection
   :param ftp_conn_id: The ftp connection id. The name or identifier for
       establishing a connection to the FTP server.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('s3_bucket', 's3_key', 'ftp_path')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
