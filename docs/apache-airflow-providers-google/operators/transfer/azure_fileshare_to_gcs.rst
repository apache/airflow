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

Transfers data from Azure FileShare Storage to Google Cloud Storage
===================================================================
The `Google Cloud Storage <https://cloud.google.com/storage/>`__  (GCS) is used to store large data from various applications.
The `Azure FileShare <https://docs.microsoft.com/en-us/azure/storage/files/>`__  (Azure FileShare) is very similar to GCS but from another provider (Azure).
This page shows how to transfer data from Azure FileShare to GCS.

Overview
--------

Data transfer
-------------

Transfer of files between Azure FileShare and Google Storage is performed with the
:class:`~airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareToGCSOperator` operator.

This operator has 1 required parameter:

* ``share_name`` - The Azure FileShare share name to transfer files from.

All parameters are described in the reference documentation - :class:`~airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareToGCSOperator`.

An example operator call might look like this:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_azure_fileshare_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_fileshare_to_gcs_basic]
    :end-before: [END howto_operator_azure_fileshare_to_gcs_basic]


Reference
---------

For further information, look at:

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
* `Azure FileShare Documentation <https://docs.microsoft.com/en-us/azure/storage/files/>`__
