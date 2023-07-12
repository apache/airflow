#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example Airflow DAG that senses document in Azure Cosmos DB.

This DAG relies on the following OS environment variables

* DATABASE_NAME - Target CosmosDB database_name.
* COLLECTION_NAME - Target CosmosDB collection_name.
* DOCUMENT_ID - The ID of the target document.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.models import DAG
from airflow.providers.microsoft.azure.sensors.cosmos import AzureCosmosDocumentSensor

DATABASE_NAME = os.environ.get("DATABASE_NAME", "example-database-name")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "example-collection-name")
DOCUMENT_ID = os.environ.get("DOCUMENT_ID", "example-document-id")


with DAG(
    "example_cosmos_document_sensor",
    start_date=datetime(2022, 8, 8),
    catchup=False,
    tags=["example"],
) as dag:
    # [START cosmos_document_sensor]
    azure_wasb_sensor = AzureCosmosDocumentSensor(
        database_name=DATABASE_NAME,
        collection_name=COLLECTION_NAME,
        document_id=DOCUMENT_ID,
        task_id="cosmos_document_sensor",
    )
    # [END cosmos_document_sensor]
