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



Transfer Data from Amazon S3 to Google Cloud Storage
====================================================
The `Google Cloud Storage <https://cloud.google.com/storage/>`__  (GCS) is used to store large
data from various applications. This is also the same with `Amazon Simple Storage Service <https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html>`__.
This page shows how to transfer data from Amazon S3 to GCS.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/howto/operator/google/_partials/prerequisite_tasks.rst

.. _howto/operator:S3ToGCSOperator:

Use the :class:`~airflow.providers.google.cloud.transfers.s3_to_gcs.S3ToGCSOperator`
to transfer data from Amazon S3 to Google Cloud Storage.

.. exampleinclude::/../airflow/providers/google/cloud/example_dags/example_s3_to_gcs.py
    :language: python
    :start-after: [START howto_transfer_s3togcs_operator]
    :end-before: [END howto_transfer_s3togcs_operator]

Reference
^^^^^^^^^

For further information, look at:

* `GCS Client Library Documentation <https://googleapis.dev/python/storage/latest/index.html>`__
* `GCS Product Documentation <https://cloud.google.com/storage/docs/>`__
* `S3 Client Library Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
* `S3 Product Documentation <https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html>`__
