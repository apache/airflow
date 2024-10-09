
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

Azure Blob Storage to Google Cloud Storage (GCS) Transfer Operator
==================================================================
The `Google Cloud Storage <https://cloud.google.com/storage/>`__  (GCS) is used to store large data from various applications.
This is also the same with `Azure Blob Storage <https://docs.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api>`__.
This page shows how to transfer data from Azure Blob Storage to GCS.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:AzureBlobStorageToGCSOperator:

Transfer Data from Azure Blob Storage to Google Cloud Storage
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Operator transfers data from Azure Blob Storage to specified bucket in Google Cloud Storage

Use the :class:`~airflow.providers.google.cloud.transfers.azure_blob_to_gcs.AzureBlobStorageToGCSOperator`
to transfer data from Azure Blob Storage to Google Cloud Storage.

Example usage:

.. exampleinclude:: /../../providers/tests/system/google/cloud/azure/example_azure_blob_to_gcs.py
    :language: python
    :start-after: [START how_to_azure_blob_to_gcs]
    :end-before: [END how_to_azure_blob_to_gcs]

Reference
^^^^^^^^^

For further information, look at:

* `GCS Client Library Documentation <https://googleapis.dev/python/storage/latest/index.html>`__
* `GCS Product Documentation <https://cloud.google.com/storage/docs/>`__
* `Azure Blob Storage Client Library Documentation <https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python>`__
