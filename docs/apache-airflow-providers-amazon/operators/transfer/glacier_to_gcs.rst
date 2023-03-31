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

========================
Amazon S3 Glacier to GCS
========================

`Amazon Glacier <https://docs.aws.amazon.com/amazonglacier/latest/dev/introduction.html>`_ is a secure, durable,
and extremely low-cost Amazon S3 cloud storage class for data archiving and long-term backup.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:GlacierToGCSOperator:

Amazon S3 Glacier To GCS transfer operator
==========================================

To transfer data from an Amazon Glacier vault to Google Cloud Storage you can use
:class:`~airflow.providers.amazon.aws.transfers.glacier_to_gcs.GlacierToGCSOperator`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_glacier_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_glacier_to_gcs]
    :end-before: [END howto_transfer_glacier_to_gcs]

.. note::
    Please be aware that GlacierToGCSOperator depends on available memory.
    Transferring large files may exhaust memory on the worker host.

Reference
---------

* `AWS boto3 library documentation for Amazon S3 Glacier <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html>`__
* `Google Cloud Storage client library <https://googleapis.dev/python/storage/latest/client.html>`__
