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


.. _howto/operator:AzureBlobStorageToTeradataOperator:


==================================
AzureBlobStorageToTeradataOperator
==================================

The purpose of ``AzureBlobStorageToTeradataOperator`` is to define tasks involving CSV, JSON and Parquet
format data transfer from an Azure Blob Storage to Teradata table.
Use the :class:`AzureBlobStorageToTeradataOperator <airflow.providers.teradata.transfers.azure_blob_to_teradata>`
to transfer data from an Azure Blob Storage to Teradata.


Transferring data in CSV format from Azure Blob Storage to Teradata
-------------------------------------------------------------------

An example usage of the AzureBlobStorageToTeradataOperator to transfer CSV data format from Azure Blob Storage
to teradata table is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_csv]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_csv]

Transferring data in JSON format from Azure Blob Storage to Teradata
--------------------------------------------------------------------

An example usage of the AzureBlobStorageToTeradataOperator to transfer JSON data format from Azure Blob Storage
to teradata table is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_json]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_json]

Transferring data in PARQUET format from Azure Blob Storage to Teradata
-----------------------------------------------------------------------

An example usage of the AzureBlobStorageToTeradataOperator to transfer PARQUET data format from Azure Blob Storage
to teradata table is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_parquet]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_parquet]

The complete ``AzureBlobStorageToTeradataOperator`` Operator DAG
----------------------------------------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide]
