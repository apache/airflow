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

# Ignore missing args provided by default_args
# type: ignore[call-arg]

"""
This is only an example DAG to highlight usage of AzureCosmosDocumentSensor to detect
if a document now exists.

You can trigger this manually with `airflow dags trigger example_cosmosdb_sensor`.

*Note: Make sure that connection `azure_cosmos_default` is properly set before running
this example.*
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.azure.operators.cosmos import AzureCosmosInsertDocumentOperator
from airflow.providers.microsoft.azure.sensors.cosmos import AzureCosmosDocumentSensor

with DAG(
    dag_id='example_azure_cosmosdb_sensor',
    default_args={'database_name': 'airflow_example_db'},
    start_date=datetime(2021, 1, 1),
    catchup=False,
    doc_md=__doc__,
    tags=['example'],
) as dag:

    t1 = AzureCosmosDocumentSensor(
        task_id='check_cosmos_file',
        collection_name='airflow_example_coll',
        document_id='airflow_checkid',
    )

    t2 = AzureCosmosInsertDocumentOperator(
        task_id='insert_cosmos_file',
        collection_name='new-collection',
        document={"id": "someuniqueid", "param1": "value1", "param2": "value2"},
    )

    t1 >> t2
