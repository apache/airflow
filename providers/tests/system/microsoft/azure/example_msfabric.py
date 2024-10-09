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
from __future__ import annotations

from datetime import datetime

from airflow import models
from airflow.assets import Asset
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator

DAG_ID = "example_msfabric"

with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    tags=["example"],
) as dag:
    # [START howto_operator_ms_fabric_create_item_schedule]
    # https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule?tabs=HTTP
    workspaces_task = MSGraphAsyncOperator(
        task_id="schedule_datapipeline",
        conn_id="powerbi",
        method="POST",
        url="workspaces/{workspaceId}/items/{itemId}/jobs/instances",
        path_parameters={
            "workspaceId": "e90b2873-4812-4dfb-9246-593638165644",
            "itemId": "65448530-e5ec-4aeb-a97e-7cebf5d67c18",
        },
        query_parameters={"jobType": "Pipeline"},
        dag=dag,
        outlets=[
            Asset(
                "workspaces/e90b2873-4812-4dfb-9246-593638165644/items/65448530-e5ec-4aeb-a97e-7cebf5d67c18/jobs/instances?jobType=Pipeline"
            )
        ],
    )
    # [END howto_operator_ms_fabric_create_item_schedule]

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
