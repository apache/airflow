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

============================
Imap Attachment to Amazon S3
============================

The ``ImapAttachmentToS3Operator`` transfers an email attachment via IMAP
protocol from an email server to an Amazon S3 Bucket.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:ImapAttachmentToS3Operator:

Imap Attachment To Amazon S3 transfer operator
==============================================

To save an email attachment via IMAP protocol from an email server to an Amazon S3 Bucket you can use
:class:`~airflow.providers.amazon.aws.transfers.imap_attachment_to_s3.ImapAttachmentToS3Operator`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_imap_attachment_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_imap_attachment_to_s3]
    :end-before: [END howto_transfer_imap_attachment_to_s3]

Reference
---------

* `IMAP Library documentation <https://docs.python.org/3/library/imaplib.html>`__
* `AWS boto3 library documentation for S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
