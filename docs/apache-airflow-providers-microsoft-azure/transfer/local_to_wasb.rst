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


Upload data from Local Filesystem to Azure Blob Storage
=======================================================
The Blob service stores text and binary data as objects in the cloud.
The Blob service offers the following three resources: the storage account, containers, and blobs.
Within your storage account, containers provide a way to organize sets of blobs.
For more information about the service visit `Azure Blob Storage API documentation <https://docs.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api>`_.
This page shows how to upload data from local filesystem to Azure Blob Storage.


.. _howto/operator:LocalFilesystemToWasbOperator:

LocalFilesystemToWasbOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`~airflow.providers.microsoft.azure.transfers.local_to_wasb.LocalFilesystemToWasbOperator` allows you to
upload data from local filesystem to Azure Blob Storage.


Below is an example of using this operator to upload a file to Azure Blob Storage.

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_local_to_wasb.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_local_to_wasb]
    :end-before: [END howto_operator_local_to_wasb]


Reference
---------

For further information, look at:

* `Azure Blob Storage Documentation <https://learn.microsoft.com/en-us/azure/storage/blobs/>`__
