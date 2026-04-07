
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

=====================================================
Google Cloud Storage to Azure Blob Storage transfer
=====================================================

`Google Cloud Storage <https://cloud.google.com/storage/>`__ and
`Azure Blob Storage <https://learn.microsoft.com/en-us/azure/storage/blobs/>`__
are object stores commonly used for data lakes and file exchange.
This guide describes copying objects from GCS into an Azure Blob container.

Install the optional dependency when using this operator:

.. code-block:: bash

    pip install 'apache-airflow-providers-microsoft-azure[google]'

Prerequisite Tasks
------------------

.. include:: ../operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GCSToAzureBlobStorageOperator:

Operator
--------

Use :class:`~airflow.providers.microsoft.azure.transfers.gcs_to_wasb.GCSToAzureBlobStorageOperator`
to list objects under a GCS ``prefix`` and upload them to a container using ``blob_prefix`` as the base path.
Use ``keep_directory_structure`` and ``flatten_structure`` the same way as
:class:`~airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSToS3Operator` (``flatten_structure`` wins when both apply).
Object keys ending with ``/`` (GCS console folder markers) are not copied.

Example:

.. code-block:: python

    copy_gcs_to_azure = GCSToAzureBlobStorageOperator(
        task_id="gcs_to_azure_blob",
        gcs_bucket="my-gcs-bucket",
        prefix="exports/daily/",
        container_name="my-container",
        blob_prefix="imports/daily",
        gcp_conn_id="google_cloud_default",
        wasb_conn_id="wasb_default",
        replace=True,
    )

Reference
---------

* `Google Cloud Storage Python client <https://cloud.google.com/python/docs/reference/storage/latest>`__
* `Azure Blob Storage client library <https://learn.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python>`__
