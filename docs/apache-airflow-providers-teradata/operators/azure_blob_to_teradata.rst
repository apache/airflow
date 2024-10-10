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
to transfer data from an Azure Blob Storage to Teradata.This operator leverages the Teradata
`READ_NOS <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Welcome-to-Native-Object-Store>`_ feature
to import data in CSV, JSON, and Parquet formats from Azure Blob Storage into Teradata.
This operator accesses data directly from the object store and generates permanent tables
within the database using READ_NOS and CREATE TABLE AS functionalities with below SQL statement.

.. code-block:: sql

    CREATE MULTISET TABLE multiset_table_name AS (
      SELECT *
      FROM (
        LOCATION='YOUR-OBJECT-STORE-URI'
        AUTHORIZATION=authorization_object
      ) AS d
    ) WITH DATA;

It facilitates data loading from both public and private object storage. For private object storage, access to the object
store can be granted via either Teradata Authorization database object or Object Store Login and Object Store Key
defined with Azure Blob Storage connection in Airflow. Conversely, for data transfer from public object storage,
no authorization or access credentials are required.

* Teradata Authorization database object access type can be used with ``teradata_authorization_name`` parameter of ``AzureBlobStorageToTeradataOperator``
* Object Store Access Key ID and Access Key Secret access type can be used with ``azure_conn_id`` parameter of ``S3ToTeradataOperator``

https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Setting-Up-Access/Setting-Access-Privileges

.. note::
   Teradata Authorization database object takes precedence if both access types defined.

Transferring data from public Azure Blob Storage to Teradata
------------------------------------------------------------

An example usage of the AzureBlobStorageToTeradataOperator to transfer CSV data format from public Azure Blob Storage to teradata table is as follows:

.. exampleinclude:: /../../providers/tests/system/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob__to_teradata_transfer_operator_howto_guide_transfer_data_public_blob_to_teradata_csv]
    :end-before: [END azure_blob__to_teradata_transfer_operator_howto_guide_transfer_data_public_blob_to_teradata_csv]

Transferring data from private Azure Blob Storage to Teradata with AWS connection
---------------------------------------------------------------------------------

An example usage of the AzureBlobStorageToTeradataOperator to transfer CSV data format from private S3 object store to teradata with AWS credentials defined as
AWS connection:

.. exampleinclude:: /../../providers/tests/system/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_access_blob_to_teradata_csv]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_access_blob_to_teradata_csv]

Transferring data from private Azure Blob Storage to Teradata with Teradata Authorization Object
------------------------------------------------------------------------------------------------
Teradata authorization database object is used to control who can access an external object store. Teradata authorization
database object should exists in Teradata database to use it in transferring data from S3 to Teradata. Refer
`Authentication for External Object Stores in Teradata <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Authentication-for-External-Object-Stores>`_

An example usage of the AzureBlobStorageToTeradataOperator to transfer CSV data format from private S3 object store to teradata with
Authorization database object defined in Teradata.

.. exampleinclude:: /../../providers/tests/system/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_authorization_blob_to_teradata_csv]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_authorization_blob_to_teradata_csv]

Transferring data in CSV format from Azure Blob Storage to Teradata
-------------------------------------------------------------------

An example usage of the AzureBlobStorageToTeradataOperator to transfer CSV data format from Azure Blob Storage
to teradata table is as follows:

.. exampleinclude:: /../../providers/tests/system/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob__to_teradata_transfer_operator_howto_guide_transfer_data_public_blob_to_teradata_csv]
    :end-before: [END azure_blob__to_teradata_transfer_operator_howto_guide_transfer_data_public_blob_to_teradata_csv]

Transferring data in JSON format from Azure Blob Storage to Teradata
--------------------------------------------------------------------

An example usage of the AzureBlobStorageToTeradataOperator to transfer JSON data format from Azure Blob Storage
to teradata table is as follows:

.. exampleinclude:: /../../providers/tests/system/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_json]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_json]

Transferring data in PARQUET format from Azure Blob Storage to Teradata
-----------------------------------------------------------------------

An example usage of the AzureBlobStorageToTeradataOperator to transfer PARQUET data format from Azure Blob Storage
to teradata table is as follows:

.. exampleinclude:: /../../providers/tests/system/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_parquet]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_parquet]

The complete ``AzureBlobStorageToTeradataOperator`` Operator DAG
----------------------------------------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../providers/tests/system/teradata/example_azure_blob_to_teradata_transfer.py
    :language: python
    :start-after: [START azure_blob_to_teradata_transfer_operator_howto_guide]
    :end-before: [END azure_blob_to_teradata_transfer_operator_howto_guide]
