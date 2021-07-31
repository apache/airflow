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


Azure Data Explorer Query Operator
=================================

.. contents::
  :depth: 1
  :local:


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:AzureDataLakeStorageDeleteOperator:

AzureDataExplorerQueryOperator
----------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.adx.AzureDataExplorerQueryOperator` to Run KQL Query on Azure Data Explorer (Kusto).


Below is an example of using this operator to run a query on azure data explorer.

.. exampleinclude:: /../../airflow/providers/microsoft/azure/example_dags/example_adx_query.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_adx_query]
    :end-before: [END howto_operator_adx_query]


Reference
---------

For further information, look at:

* `Azure Data lake Explorer Documentation <https://docs.microsoft.com/en-us/azure/kusto/api/rest/response2>`__
