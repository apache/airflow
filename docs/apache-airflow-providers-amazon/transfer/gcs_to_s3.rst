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

===================================================
Google Cloud Storage to Amazon S3 transfer operator
===================================================

Use the ``GCSToS3Operator`` transfer to copy the data from Google Cloud Storage to Amazon Simple Storage Service (S3).

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:GCSToS3Operator:

Google Cloud Storage to Amazon S3
=================================

To copy data from a Google Cloud Storage bucket to an Amazon S3 bucket you can use
:class:`~airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSToS3Operator`

Example usage:

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_gcs_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_gcs_to_s3]
    :end-before: [END howto_transfer_gcs_to_s3]

Reference
---------

* `Google Cloud Storage client library <https://googleapis.dev/python/storage/latest/client.html>`__
* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
