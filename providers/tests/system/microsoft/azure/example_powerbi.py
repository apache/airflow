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
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator
from airflow.providers.microsoft.azure.sensors.msgraph import MSGraphSensor

DAG_ID = "example_powerbi"

with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    tags=["example"],
) as dag:
    # [START howto_operator_powerbi_workspaces]
    workspaces_task = MSGraphAsyncOperator(
        task_id="workspaces",
        conn_id="powerbi",
        url="myorg/admin/workspaces/modified",
        result_processor=lambda context, response: list(map(lambda workspace: workspace["id"], response)),
    )
    # [END howto_operator_powerbi_workspaces]

    # [START howto_operator_powerbi_workspaces_info]
    workspaces_info_task = MSGraphAsyncOperator(
        task_id="get_workspace_info",
        conn_id="powerbi",
        url="myorg/admin/workspaces/getInfo",
        method="POST",
        query_parameters={
            "lineage": True,
            "datasourceDetails": True,
            "datasetSchema": True,
            "datasetExpressions": True,
            "getArtifactUsers": True,
        },
        data={"workspaces": workspaces_task.output},
        result_processor=lambda context, response: {"scanId": response["id"]},
    )
    # [END howto_operator_powerbi_workspaces_info]

    # [START howto_sensor_powerbi_scan_status]
    check_workspace_status_task = MSGraphSensor.partial(
        task_id="check_workspaces_status",
        conn_id="powerbi_api",
        url="myorg/admin/workspaces/scanStatus/{scanId}",
        timeout=350.0,
    ).expand(path_parameters=workspaces_info_task.output)
    # [END howto_sensor_powerbi_scan_status]

    # [START howto_operator_powerbi_refresh_dataset]
    refresh_dataset_task = MSGraphAsyncOperator(
        task_id="refresh_dataset",
        conn_id="powerbi_api",
        url="myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes",
        method="POST",
        path_parameters={
            "workspaceId": "9a7e14c6-9a7d-4b4c-b0f2-799a85e60a51",
            "datasetId": "ffb6096e-d409-4826-aaeb-b5d4b165dc4d",
        },
        data={"type": "full"},  # Needed for enhanced refresh
        result_processor=lambda context, response: response["requestid"],
    )

    refresh_dataset_history_task = MSGraphSensor(
        task_id="refresh_dataset_history",
        conn_id="powerbi_api",
        url="myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes/{refreshId}",
        path_parameters={
            "workspaceId": "9a7e14c6-9a7d-4b4c-b0f2-799a85e60a51",
            "datasetId": "ffb6096e-d409-4826-aaeb-b5d4b165dc4d",
            "refreshId": refresh_dataset_task.output,
        },
        timeout=350.0,
        event_processor=lambda context, event: event["status"] == "Completed",
    )
    # [END howto_operator_powerbi_refresh_dataset]

    workspaces_task >> workspaces_info_task >> check_workspace_status_task
    refresh_dataset_task >> refresh_dataset_history_task

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
