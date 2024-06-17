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


.. _howto/operator:S3ToTeradataOperator:


============================
S3ToTeradataOperator
============================

The purpose of ``S3ToTeradataOperator`` is to define tasks involving CSV, JSON and Parquet
format data transfer from an AWS Simple Storage Service (S3) to Teradata table. This operator uses
Teradata READ_NOS feature to transfer data from an AWS Simple Storage Service (S3) to Teradata table.
READ_NOS is a table operator in Teradata Vantage that allows users to list external files at a specified location.
For more details, see `READ_NOS Functionality <https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Reading-Data/Examples-For-DBAs-and-Advanced-Users/Loading-External-Data-into-the-Database/Loading-External-Data-into-the-Database-Using-READ_NOS-and-CREATE-TABLE-AS>`_

Use the :class:`S3ToTeradataOperator <airflow.providers.teradata.transfers.s3_to_teradata>`
to transfer data from S3 to Teradata.

 .. note::
  The current version of ``S3ToTeradataOperator`` does not support accessing AWS S3 with Security Token Service (STS) temporary credentials. Instead, it exclusively supports accessing with long-term credentials.


Transferring data in CSV format from S3 to Teradata
---------------------------------------------------

An example usage of the S3ToTeradataOperator to transfer CSV data format from S3 to teradata table is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_s3_to_teradata_transfer.py
    :language: python
    :start-after: [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_csv]
    :end-before: [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_csv]

Transferring data in JSON format from S3 to Teradata
----------------------------------------------------

An example usage of the S3ToTeradataOperator to transfer JSON data format from S3 to teradata table is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_s3_to_teradata_transfer.py
    :language: python
    :start-after: [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_json]
    :end-before: [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_json]

Transferring data in PARQUET format from S3 to Teradata
-------------------------------------------------------

An example usage of the S3ToTeradataOperator to transfer PARQUET data format from S3 to teradata table is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_s3_to_teradata_transfer.py
    :language: python
    :start-after: [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_parquet]
    :end-before: [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_parquet]

The complete ``S3ToTeradataOperator`` Operator DAG
--------------------------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/teradata/example_s3_to_teradata_transfer.py
    :language: python
    :start-after: [START s3_to_teradata_transfer_operator_howto_guide]
    :end-before: [END s3_to_teradata_transfer_operator_howto_guide]
