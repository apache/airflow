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

Azure Analysis Services Operators
==================================
Azure Analysis Services is a fully managed platform as a service (PaaS) that provides
enterprise-grade data models in the cloud.

.. _howto/operator:AzureAnalysisServicesRefreshOperator:

AzureAnalysisServicesRefreshOperator
--------------------------------------
Use the :class:`~airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesRefreshOperator`
to trigger a model refresh on an Azure Analysis Services database.

By default, the operator waits for the refresh to complete before finishing. To trigger a refresh
without waiting, set ``wait_for_termination`` to ``False`` — the refresh ID will be pushed to XCom
and you can use :class:`~airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesSensor`
to wait for it later.

The operator also supports deferrable mode (``deferrable=True``) to free up worker slots while polling
for completion on the Airflow Triggerer.

For connection setup, see :ref:`howto/connection:azure_analysis_services`.

Example usage:

.. code-block:: python

    from airflow.providers.microsoft.azure.operators.analysis_services import (
        AzureAnalysisServicesRefreshOperator,
    )

    refresh = AzureAnalysisServicesRefreshOperator(
        task_id="refresh_model",
        server_name="myserver",
        database="AdventureWorks",
        azure_analysis_services_conn_id="azure_analysis_services_default",
        refresh_type="full",
    )

.. _howto/operator:AzureAnalysisServicesSensor:

AzureAnalysisServicesSensor
-----------------------------
Use the :class:`~airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesSensor`
to poll an existing Azure Analysis Services model refresh until it reaches a terminal state.

This is useful when you trigger a refresh with ``wait_for_termination=False`` and want to check its
status from a separate task.

.. spelling:word-list::
    asazure
