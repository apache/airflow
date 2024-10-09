
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

=================================================
Amazon S3 to Azure Blob Storage Transfer Operator
=================================================

The Blob service stores text and binary data as objects in the cloud.
The Blob service offers the following three resources: the storage account, containers, and blobs.
Within your storage account, containers provide a way to organize sets of blobs.
For more information about the service visit `Azure Blob Storage API documentation <https://docs.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api>`_.
This page shows how to upload data from local filesystem to Azure Blob Storage.

Use the ``S3ToWasbOperator`` transfer to copy the data from Amazon Simple Storage Service (S3) to Azure Blob Storage.

Prerequisite Tasks
------------------

.. include:: ../operators/_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:S3ToWasbOperator:


Transfer Data from Amazon S3 to Blob Storage
============================================

To copy data from an Amazon AWS S3 Bucket to an Azure Blob Storage container, the following operator can be used:
:class:`~airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3ToWasbOperator`

Example usage:

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_s3_to_wasb.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_s3_to_wasb]
    :end-before: [END howto_transfer_s3_to_wasb]

Reference
---------

For further information, please refer to the following links:

* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
* `Azure Blob Storage client library <https://learn.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python>`__
