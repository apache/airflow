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
Azure Blob Storage to Amazon S3 transfer operator
===================================================

Use the ``AzureBlobStorageToS3Operator`` transfer to copy the data from Azure Blob Storage to Amazon Simple Storage Service (S3).

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:AzureBlobStorageToS3Operator:

Azure Blob Storage to Amazon S3
=================================

To copy data from an Azure Blob Storage container to an Amazon S3 bucket you can use
:class:`~airflow.providers.amazon.aws.transfers.azure_blob_to_s3.AzureBlobStorageToS3Operator`

Example usage:

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_azure_blob_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_azure_blob_to_s3]
    :end-before: [END howto_transfer_azure_blob_to_s3]

Reference
---------

* `Azure Blob Storage client library <https://learn.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python>`__
* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
