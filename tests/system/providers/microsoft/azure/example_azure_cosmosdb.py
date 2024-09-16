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
This is only an example DAG to highlight usage of AzureCosmosDocumentSensor to detect
if a document now exists.

You can trigger this manually with `airflow dags trigger example_cosmosdb_sensor`.

*Note: Make sure that connection `azure_cosmos_default` is properly set before running
this example.*
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.azure.operators.cosmos import AzureCosmosInsertDocumentOperator
from airflow.providers.microsoft.azure.sensors.cosmos import AzureCosmosDocumentSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_azure_cosmosdb_sensor"

with DAG(
    dag_id=DAG_ID,
    default_args={"database_name": "airflow_example_db"},
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["example"],
) as dag:
    # [START cosmos_document_sensor]
    t1 = AzureCosmosDocumentSensor(
        task_id="check_cosmos_file",
        collection_name="airflow_example_coll",
        document_id="airflow_checkid",
        database_name="database_name",
    )
    # [END cosmos_document_sensor]

    t2 = AzureCosmosInsertDocumentOperator(
        task_id="insert_cosmos_file",
        collection_name="new-collection",
        document={"id": "someuniqueid", "param1": "value1", "param2": "value2"},
        database_name="database_name",
    )

    t1 >> t2

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
