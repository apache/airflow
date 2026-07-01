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

By default, the operator waits for the refresh to complete before finishing.

The ``refresh_type`` parameter controls what processing is performed:

.. list-table::
   :header-rows: 1

   * - Value
     - Description
   * - ``full``
     - Refreshes data and recalculates all dependencies (default)
   * - ``clearValues``
     - Clears calculated values without re-ingesting data
   * - ``calculate``
     - Recalculates all dependents without refreshing data
   * - ``dataOnly``
     - Refreshes data only, skips recalculation
   * - ``automatic``
     - Azure determines what needs refreshing based on metadata state
   * - ``defragment``
     - Defragments the database without changing data

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_analysis_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_analysis_services_refresh]
    :end-before: [END howto_operator_azure_analysis_services_refresh]

To run in deferrable mode and free up worker slots while polling on the Airflow Triggerer:

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_analysis_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_analysis_services_refresh_deferrable]
    :end-before: [END howto_operator_azure_analysis_services_refresh_deferrable]

.. _howto/operator:AzureAnalysisServicesSensor:

AzureAnalysisServicesSensor
-----------------------------
Use the :class:`~airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesSensor`
to poll an existing Azure Analysis Services model refresh until it reaches a terminal state.

This is useful when you trigger a refresh with ``wait_for_termination=False`` and want to monitor
its status from a separate downstream task. The ``refresh_id`` pushed to XCom by the operator can
be passed directly to the sensor.

For connection setup, see :ref:`howto/connection:azure_analysis_services`.

Reference
---------

For further information, please refer to the Microsoft documentation:

  * `Azure Analysis Services Documentation <https://learn.microsoft.com/en-us/azure/analysis-services/>`__
  * `Azure Analysis Services REST API <https://learn.microsoft.com/en-us/rest/api/analysisservices/>`__

.. spelling:word-list::
    asazure
    Defragments
