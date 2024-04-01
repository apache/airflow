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



============================
Amazon S3 to Teradata
============================

Use the ``S3ToTeradataOperator`` transfer to copy CSV, JSON and Parquet format data from an Amazon Simple Storage Service (S3) file into Teradata table.

Operators
---------

.. _howto/operator:S3ToTeradataOperator:

Amazon S3 To Teradata transfer operator
==============================================

This operator loads CSV, JSON and Parquet format data from Amazon S3 to Teradata table.

Using the Operator
^^^^^^^^^^^^^^^^^^

To transfer CSV, JSON and Parquet data from Amazon S3 to Teradata, use the
:class:`~airflow.providers.teradata.transfers.s3_to_teradata.S3ToTeradataOperator`.

An example of executing a simple query is as follows:

.. exampleinclude:: /../../airflow/providers/teradata/example_dags/example_s3_to_teradata_transfer_operator.py
    :language: python
    :start-after: [START howto_transfer_operator_s3_to_teradata]
    :end-before: [END howto_transfer_operator_s3_to_teradata]
