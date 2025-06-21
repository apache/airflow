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

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_logging_sink import (
    CloudLoggingCreateSinkOperator,
    CloudLoggingDeleteSinkOperator,
    CloudLoggingListSinksOperator,
    CloudLoggingUpdateSinkOperator,
)

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")

SINK_NAME = "example-airflow-test-sink"
DESTINATION = "storage.googleapis.com/test-log-sink-af"
CONN_ID = "google_cloud_default"

EXCLUSION_FILTER = [
    {
        "name": "exclude-debug",
        "description": "Skip debug logs",
        "filter": "severity=DEBUG",
        "disabled": True,
    },
    {
        "name": "exclude-cloudsql",
        "description": "Skip CloudSQL logs",
        "filter": 'resource.type="cloudsql_database"',
        "disabled": False,
    },
]

with DAG(
    dag_id="example_google_cloud_logging_sink",
    description="Example DAG showing usage of all Cloud Logging Sink operators",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "gcp", "cloud-logging"],
) as dag:
    # [START howto_operator_cloud_logging_create_sink]
    create_sink = CloudLoggingCreateSinkOperator(
        task_id="create_sink",
        project_id=PROJECT_ID,
        sink_name=SINK_NAME,
        exclusion_filter=EXCLUSION_FILTER,
        description="Initial sink for storing debug and CloudSQL logs.",
        destination=DESTINATION,
        gcp_conn_id=CONN_ID,
    )
    # [END howto_operator_cloud_logging_create_sink]

    # [START howto_operator_cloud_logging_update_sink]
    update_sink = CloudLoggingUpdateSinkOperator(
        task_id="update_sink",
        sink_name=SINK_NAME,
        project_id=PROJECT_ID,
        description="Updated sink description",
        filter_='resource.type="gce_instance"',
        disabled=False,
        unique_writer_identity=True,
        gcp_conn_id=CONN_ID,
    )
    # [END howto_operator_cloud_logging_update_sink]

    # [START howto_operator_cloud_logging_list_sinks]
    list_sinks_after = CloudLoggingListSinksOperator(
        task_id="list_sinks_after_update",
        project_id=PROJECT_ID,
        gcp_conn_id=CONN_ID,
    )
    # [END howto_operator_cloud_logging_list_sinks]

    # [START howto_operator_cloud_logging_delete_sink]
    delete_sink = CloudLoggingDeleteSinkOperator(
        task_id="delete_sink",
        sink_name=SINK_NAME,
        project_id=PROJECT_ID,
        gcp_conn_id=CONN_ID,
    )
    # [END howto_operator_cloud_logging_delete_sink]

    (create_sink >> update_sink >> list_sinks_after >> delete_sink)

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
