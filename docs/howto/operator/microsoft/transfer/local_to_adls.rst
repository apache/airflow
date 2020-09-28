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


Upload data from Local Filesystem to Azure Data Lake
====================================================
The `Azure Data Lake <https://azure.microsoft.com/en-us/solutions/data-lake/>`__  (ADL)  make it easy to store data of
any size, shape, and speed.
This page shows how to upload data from local filesystem to ADL.

.. contents::
  :depth: 1
  :local:


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/howto/operator/microsoft/_partials/prerequisite_tasks.rst

.. _howto/operator:LocalToAzureDataLakeStorageOperator:

LocalToAzureDataLakeStorageOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`~airflow.providers.microsoft.azure.transfers.local_to_adls.LocalToAzureDataLakeStorageOperator` allows you to
upload data from local filesystem to ADL.


Below is an example of using this operator to upload a file to ADL.

.. exampleinclude:: /../airflow/providers/microsoft/azure/example_dags/example_local_to_adls.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_local_to_adls]
    :end-before: [END howto_operator_local_to_adls]


Reference
---------

For further information, look at:

* `Azure Data lake Storage Documentation <https://docs.microsoft.com/en-us/azure/data-lake-store/>`__
