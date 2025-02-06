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

:py:mod:`airflow.providers.amazon.aws.transfers.ftp_to_s3`
==========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.ftp_to_s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.ftp_to_s3.FTPToS3Operator




.. py:class:: FTPToS3Operator(*, ftp_path, s3_bucket, s3_key, ftp_filenames = None, s3_filenames = None, ftp_conn_id = 'ftp_default', aws_conn_id = 'aws_default', replace = False, encrypt = False, gzip = False, acl_policy = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Transfer of one or more files from an FTP server to S3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:FTPToS3Operator`

   :param ftp_path: The ftp remote path. For one file it is mandatory to include the file as well.
       For multiple files, it is the route where the files will be found.
   :param s3_bucket: The targeted s3 bucket in which to upload the file(s).
   :param s3_key: The targeted s3 key. For one file it must include the file path. For several,
       it must end with "/".
   :param ftp_filenames: Only used if you want to move multiple files. You can pass a list
       with exact filenames present in the ftp path, or a prefix that all files must meet. It
       can also be the string '*' for moving all the files within the ftp path.
   :param s3_filenames: Only used if you want to move multiple files and name them different from
       the originals from the ftp. It can be a list of filenames or file prefix (that will replace
       the ftp prefix).
   :param ftp_conn_id: The ftp connection id. The name or identifier for
       establishing a connection to the FTP server.
   :param aws_conn_id: The s3 connection id. The name or identifier for
       establishing a connection to S3.
   :param replace: A flag to decide whether or not to overwrite the key
       if it already exists. If replace is False and the key exists, an
       error will be raised.
   :param encrypt: If True, the file will be encrypted on the server-side
       by S3 and will be stored in an encrypted form while at rest in S3.
   :param gzip: If True, the file will be compressed locally
   :param acl_policy: String specifying the canned ACL policy for the file being
       uploaded to the S3 bucket.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('ftp_path', 's3_bucket', 's3_key', 'ftp_filenames', 's3_filenames')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
