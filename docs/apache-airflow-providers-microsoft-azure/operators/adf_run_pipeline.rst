
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

Azure Data Factory Operators
============================
Azure Data Factory is Azure's cloud ETL service for scale-out serverless data integration and data transformation.
It offers a code-free UI for intuitive authoring and single-pane-of-glass monitoring and management.

.. _howto/operator:AzureDataFactoryRunPipelineOperator:

AzureDataFactoryRunPipelineOperator
-----------------------------------
Use the :class:`~airflow.providers.microsoft.azure.operators.data_factory.AzureDataFactoryRunPipelineOperator` to execute a pipeline within a data factory.
By default, the operator will periodically check on the status of the executed pipeline to terminate with a "Succeeded" status.
This functionality can be disabled for an asynchronous wait -- typically with the :class:`~airflow.providers.microsoft.azure.sensors.data_factory.AzureDataFactoryPipelineRunSensor` -- by setting ``wait_for_termination`` to False.

Below is an example of using this operator to execute an Azure Data Factory pipeline.

  .. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_adf_run_pipeline.py
      :language: python
      :dedent: 0
      :start-after: [START howto_operator_adf_run_pipeline]
      :end-before: [END howto_operator_adf_run_pipeline]

Here is a different example of using this operator to execute a pipeline but coupled with the :class:`~airflow.providers.microsoft.azure.sensors.data_factory.AzureDataFactoryPipelineRunSensor` to perform an asynchronous wait.

    .. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_adf_run_pipeline.py
        :language: python
        :dedent: 0
        :start-after: [START howto_operator_adf_run_pipeline_async]
        :end-before: [END howto_operator_adf_run_pipeline_async]

Reference
---------

For further information, please refer to the Microsoft documentation:

  * `Azure Data Factory Documentation <https://docs.microsoft.com/en-us/azure/data-factory/>`__
