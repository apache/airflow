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

================
Amazon S3 to FTP
================

Use the ``S3ToFTPOperator`` transfer to copy data from an Amazon Simple Storage Service (S3) file into a remote file
using FTP protocol.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:S3ToFTPOperator:

Amazon S3 To FTP transfer operator
==================================

This operator copies data from Amazon S3 to a FTP server.

To get more information about this operator visit:
:class:`~airflow.providers.amazon.aws.transfers.s3_to_ftp.S3ToFTPOperator`

Example usage:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_s3_to_ftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_s3_to_ftp]
    :end-before: [END howto_transfer_s3_to_ftp]

Reference
---------

* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
