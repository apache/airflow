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


Microsoft Graph API Operators
=============================

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:MSGraphAsyncOperator:

MSGraphAsyncOperator
----------------------------------
Use the
:class:`~airflow.providers.microsoft.azure.operators.msgraph.MSGraphAsyncOperator` to call Microsoft Graph API.


Below is an example of using this operator to get a Sharepoint site.

.. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_msgraph.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_graph_site]
    :end-before: [END howto_operator_graph_site]

Below is an example of using this operator to get a Sharepoint site pages.

.. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_msgraph.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_graph_site_pages]
    :end-before: [END howto_operator_graph_site_pages]

Below is an example of using this operator to get PowerBI workspaces.

.. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_powerbi.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_workspaces]
    :end-before: [END howto_operator_powerbi_workspaces]

Below is an example of using this operator to get PowerBI workspaces info.

.. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_powerbi.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_workspaces_info]
    :end-before: [END howto_operator_powerbi_workspaces_info]

Below is an example of using this operator to refresh PowerBI dataset.

.. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_powerbi.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_refresh_dataset]
    :end-before: [END howto_operator_powerbi_refresh_dataset]

Below is an example of using this operator to create an item schedule in Fabric.

.. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_msfabric.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_ms_fabric_create_item_schedule]
    :end-before: [END howto_operator_ms_fabric_create_item_schedule]


Reference
---------

For further information, look at:

* `Use the Microsoft Graph API <https://learn.microsoft.com/en-us/graph/use-the-api/>`__
* `Using the Power BI REST APIs <https://learn.microsoft.com/en-us/rest/api/power-bi/>`__
* `Using the Fabric REST APIs <https://learn.microsoft.com/en-us/rest/api/fabric/articles/using-fabric-apis/>`__
