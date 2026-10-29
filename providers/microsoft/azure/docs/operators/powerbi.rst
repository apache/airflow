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


Microsoft Power BI Operators
=============================
`Microsoft Power BI <https://powerbi.microsoft.com/>`__, is a unified, scalable platform for self-service and enterprise business intelligence(BI).


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:PowerBIDatasetRefreshOperator:

PowerBIDatasetRefreshOperator
----------------------------------

To trigger a refresh for the specified dataset from the specified workspace, use the :class:`~airflow.providers.microsoft.azure.operators.powerbi.PowerBIDatasetRefreshOperator`.


.. exampleinclude:: /../tests/system/microsoft/azure/example_powerbi_dataset_refresh.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_refresh_async]
    :end-before: [END howto_operator_powerbi_refresh_async]

Durable execution
~~~~~~~~~~~~~~~~~

The operator is :doc:`durable <apache-airflow:core-concepts/resumable-tasks>` by default: it starts the
refresh from the worker and keeps its id in the
:doc:`task state store <apache-airflow:core-concepts/task-state-store>` before it defers. A retry reconnects
to a refresh that is still in progress and does not start a new one once it has completed. A refresh that
failed, or that is no longer in the dataset's refresh history, is started again. Durable execution
requires Airflow 3.3 or newer.

Two things differ from earlier versions of the provider when the operator is durable:

* The refresh is started by the worker, so the workers need network access to ``api.powerbi.com`` too.
  The triggerer still polls the refresh.
* The refresh is not cancelled when polling it fails, so the retry can reconnect to it.

Set ``durable=False`` to keep the earlier behaviour: the trigger starts the refresh, so only the triggerer
talks to Power BI, every try starts a new refresh, and it is cancelled when polling fails. Below Airflow
3.3 the operator behaves as with ``durable=False``.

.. _howto/operator:PowerBIDatasetListOperator:

PowerBIDatasetListOperator
----------------------------------

To list all available and discoverable datasets from the specified workspace, use the :class:`~airflow.providers.microsoft.azure.operators.powerbi.PowerBIDatasetListOperator`.


.. exampleinclude:: /../tests/system/microsoft/azure/example_powerbi_dataset_list.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_dataset_list_async]
    :end-before: [END howto_operator_powerbi_dataset_list_async]

.. _howto/operator:PowerBIWorkspaceListOperator:

PowerBIWorkspaceListOperator
----------------------------------

To list all available and discoverable workspaces for the tenant, use the :class:`~airflow.providers.microsoft.azure.operators.powerbi.PowerBIWorkspaceListOperator`.

.. exampleinclude:: /../tests/system/microsoft/azure/example_powerbi_workspace_list.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_powerbi_workspace_list_async]
    :end-before: [END howto_operator_powerbi_workspace_list_async]

Reference
---------

For further information, look at:

* `Use the Microsoft Graph API <https://learn.microsoft.com/en-us/graph/use-the-api/>`__
* `Using the Power BI REST APIs <https://learn.microsoft.com/en-us/rest/api/power-bi/>`__
* `Using the Fabric REST APIs <https://learn.microsoft.com/en-us/rest/api/fabric/articles/using-fabric-apis/>`__
