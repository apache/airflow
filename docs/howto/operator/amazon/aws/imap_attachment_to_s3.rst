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


.. _howto/operator:ImapAttachmentToS3Operator:

Imap Attachment To S3 Operator
==============================

.. contents::
  :depth: 1
  :local:

Overview
--------

The ``ImapAttachmentToS3Operator`` can transfer an email attachment via IMAP
protocol from a mail server to S3 Bucket.

An example dag ``example_imap_attachment_to_s3.py`` is provided which showcase the
:class:`~airflow.providers.amazon.aws.transfers.imap_attachment_to_s3.ImapAttachmentToS3Operator`
in action.

example_imap_attachment_to_s3.py
--------------------------------

Purpose
"""""""
This is an example dag for using ``ImapAttachmentToS3Operator`` to transfer an email attachment via IMAP
protocol from a mail server to S3 Bucket.

Environment variables
"""""""""""""""""""""

These examples rely on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_imap_attachment_to_s3.py
    :language: python
    :start-after: [START howto_operator_imap_attachment_to_s3_env_variables]
    :end-before: [END howto_operator_imap_attachment_to_s3_env_variables]

Transfer Mail Attachments via IMAP to S3
""""""""""""""""""""""""""""""""""""""""

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_imap_attachment_to_s3.py
    :language: python
    :start-after: [START howto_operator_imap_attachment_to_s3_task_1]
    :end-before: [END howto_operator_imap_attachment_to_s3_task_1]

Reference
---------

For further information, look at:

* `IMAP Library Documentation <https://docs.python.org/3.6/library/imaplib.html>`__
* `AWS boto3 Library Documentation for S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
