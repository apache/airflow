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

Azure Cosmos DB
==================
Cosmos Database (DB) is a globally distributed, low latency, multi-model database for managing data at large scales.
It is a cloud-based NoSQL database offered as a PaaS (Platform as a Service) from Microsoft Azure.
It is a highly available, high throughput, reliable database and is often called a serverless database.
Cosmos database contains the Azure Document DB and is available everywhere.

Azure Cosmos Document Sensor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Checks for the existence of a document which matches the given query in CosmosDB.
:class:`~airflow.providers.microsoft.azure.sensors.cosmos.AzureCosmosDocumentSensor`

.. exampleinclude:: /../../providers/tests/system/microsoft/azure/example_azure_cosmosdb.py
    :language: python
    :dedent: 4
    :start-after: [START cosmos_document_sensor]
    :end-before: [END cosmos_document_sensor]
