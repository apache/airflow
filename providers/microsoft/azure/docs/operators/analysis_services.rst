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

Microsoft Azure Analysis Services Operators
===========================================

Use these components to start and monitor asynchronous model refresh operations in Microsoft Azure Analysis
Services. Configure an :ref:`Azure Analysis Services connection <howto/connection:azure_analysis_services>`
before using them.

Prerequisite Tasks
------------------

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:AzureAnalysisServicesRefreshOperator:

Start a model refresh
---------------------

Use :class:`~airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesRefreshOperator`
with ``wait_for_termination=False`` to start a refresh and immediately return its refresh ID.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_analysis_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_analysis_services_refresh]
    :end-before: [END howto_operator_azure_analysis_services_refresh]

By default, ``wait_for_termination=True`` and ``deferrable=False``, so the operator waits synchronously for
the refresh to finish.

Wait with a deferrable operator
-------------------------------

Set ``deferrable=True`` to release the worker slot while the triggerer polls the refresh status. The
``timeout`` parameter limits the entire wait, while ``request_timeout`` limits each REST request.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_analysis_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_analysis_services_deferrable]
    :end-before: [END howto_operator_azure_analysis_services_deferrable]

.. _howto/sensor:AzureAnalysisServicesSensor:

Wait with a sensor
------------------

The operator return value can be passed directly to
:class:`~airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesSensor`.
This separates starting the refresh from waiting for it.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_analysis_services.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_azure_analysis_services_refresh]
    :end-before: [END howto_sensor_azure_analysis_services_refresh]

Azure Analysis Services accepts only one active refresh for a model. A second request returns HTTP 409, so
serialize refresh tasks that target the same model, for example with task dependencies or an Airflow pool.

Reference
---------

For more information, see `Asynchronous refresh with the REST API
<https://learn.microsoft.com/en-us/analysis-services/azure-analysis-services/analysis-services-async-refresh>`__.
