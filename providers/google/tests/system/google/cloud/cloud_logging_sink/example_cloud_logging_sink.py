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

# [START howto_operator_import_protobuf_obj]
from google.cloud.logging_v2.types import LogSink
from google.protobuf.field_mask_pb2 import FieldMask

# [END howto_operator_import_protobuf_obj]
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_logging_sink import (
    CloudLoggingCreateSinkOperator,
    CloudLoggingDeleteSinkOperator,
    CloudLoggingListSinksOperator,
    CloudLoggingUpdateSinkOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "gcp_cloud_logging_sink"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

SINK_NAME = "example-airflow-test-sink"
CONN_ID = "google_cloud_default"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "gcp", "cloud-logging"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    # [START howto_operator_cloud_logging_create_sink_native_obj]
    create_sink = CloudLoggingCreateSinkOperator(
        task_id="create_sink",
        project_id=PROJECT_ID,
        sink_config={
            "name": SINK_NAME,
            "destination": f"storage.googleapis.com/{BUCKET_NAME}",
            "description": "Create with full sink_config",
            "filter": "severity>=INFO",
            "disabled": False,
            "exclusions": [
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
            ],
        },
        gcp_conn_id=CONN_ID,
    )
    # [END howto_operator_cloud_logging_create_sink_native_obj]

    # [START howto_operator_cloud_logging_update_sink_protobuf_obj]
    update_sink_config = CloudLoggingUpdateSinkOperator(
        task_id="update_sink_config",
        sink_name=SINK_NAME,
        project_id=PROJECT_ID,
        sink_config=LogSink(
            {
                "description": "Update #1: GCE logs only",
                "filter": 'resource.type="gce_instance"',
                "disabled": False,
            }
        ),
        update_mask=FieldMask(paths=["description", "filter", "disabled"]),
        unique_writer_identity=True,
        gcp_conn_id=CONN_ID,
    )
    # [END howto_operator_cloud_logging_update_sink_protobuf_obj]

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

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (create_bucket >> create_sink >> update_sink_config >> list_sinks_after >> delete_sink >> delete_bucket)

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
