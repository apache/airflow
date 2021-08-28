
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

.. contents::
  :depth: 1
  :local:

.. _howto/operator:AzureDataFactoryRunPipelineOperator:

AzureDataFactoryRunPipelineOperator
-----------------------------------
Use the :class:`~airflow.providers.microsoft.azure.operators.azure_data_factory.AzureDataFactoryRunPipelineOperator` to execute a pipeline within a data factory.
By default, the operator will check on the status of the executed pipeline and mirror its result (i.e. succeed if the pipeline succeeds or fail if the pipeline fails or is canceled).
This functionality can be disabled for an asynchronous wait -- typically with the :class:`~airflow.providers.microsoft.azure.sensors.azure_data_factory.AzureDataFactoryPipelineRunSensor` -- by setting ``wait_for_completion`` to False.

Below is an example of using this operator to execute an Azure Data Factory pipeline.

  .. exampleinclude:: /../../airflow/providers/microsoft/azure/example_dags/example_adf_run_pipeline.py
      :language: python
      :dedent: 0
      :start-after: [START howto_operator_adf_run_pipeline]
      :end-before: [END howto_operator_adf_run_pipeline]

Reference
---------

For further information, please refer to the Microsoft documentation:

  * `Azure Data Factory Documentation <https://docs.microsoft.com/en-us/azure/data-factory/>`__
