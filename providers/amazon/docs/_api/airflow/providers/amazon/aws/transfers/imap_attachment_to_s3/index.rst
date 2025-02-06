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

:py:mod:`airflow.providers.amazon.aws.transfers.imap_attachment_to_s3`
======================================================================

.. py:module:: airflow.providers.amazon.aws.transfers.imap_attachment_to_s3

.. autoapi-nested-parse::

   This module allows you to transfer mail attachments from a mail server into s3 bucket.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.imap_attachment_to_s3.ImapAttachmentToS3Operator




.. py:class:: ImapAttachmentToS3Operator(*, imap_attachment_name, s3_bucket, s3_key, imap_check_regex = False, imap_mail_folder = 'INBOX', imap_mail_filter = 'All', s3_overwrite = False, imap_conn_id = 'imap_default', aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Transfers a mail attachment from a mail server into s3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:ImapAttachmentToS3Operator`

   :param imap_attachment_name: The file name of the mail attachment that you want to transfer.
   :param s3_bucket: The targeted s3 bucket. This is the S3 bucket where the file will be downloaded.
   :param s3_key: The destination file name in the s3 bucket for the attachment.
   :param imap_check_regex: If set checks the `imap_attachment_name` for a regular expression.
   :param imap_mail_folder: The folder on the mail server to look for the attachment.
   :param imap_mail_filter: If set other than 'All' only specific mails will be checked.
       See :py:meth:`imaplib.IMAP4.search` for details.
   :param s3_overwrite: If set overwrites the s3 key if already exists.
   :param imap_conn_id: The reference to the connection details of the mail server.
   :param aws_conn_id: AWS connection to use.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('imap_attachment_name', 's3_key', 'imap_mail_filter')



   .. py:method:: execute(context)

      Execute the transfer from the email server (via imap) into s3.

      :param context: The context while executing.
