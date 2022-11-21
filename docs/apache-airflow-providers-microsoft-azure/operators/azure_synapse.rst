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

Azure Synapse Operators
=======================
Azure Synapse Analytics is a limitless analytics service that brings together data integration,
enterprise data warehousing and big data analytics. It gives you the freedom to query data on your terms,
using either serverless or dedicated options—at scale.
Azure Synapse brings these worlds together with a unified experience to ingest,
explore, prepare, transform, manage and serve data for immediate BI and machine learning needs.

.. _howto/operator:AzureSynapseRunSparkBatchOperator:

AzureSynapseRunSparkBatchOperator
-----------------------------------
Use the :class:`~airflow.providers.microsoft.azure.operators.synapse.AzureSynapseRunSparkBatchOperator` to execute a
spark application within Synapse Analytics.
By default, the operator will periodically check on the status of the executed Spark job to
terminate with a "Succeeded" status.

Below is an example of using this operator to execute a Spark application on Azure Synapse.

  .. exampleinclude:: /../../tests/system/providers/microsoft/azure/example_azure_synapse.py
      :language: python
      :dedent: 4
      :start-after: [START howto_operator_azure_synapse]
      :end-before: [END howto_operator_azure_synapse]


Reference
---------

For further information, please refer to the Microsoft documentation:

  * `Azure Synapse Analytics Documentation <https://docs.microsoft.com/en-us/azure/synapse-analytics//>`__
