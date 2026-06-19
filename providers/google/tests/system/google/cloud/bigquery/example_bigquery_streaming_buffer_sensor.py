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
Example Airflow DAG exercising ``BigQueryStreamingBufferEmptySensor``.

BigQuery's streaming buffer can take up to ~90 minutes to flush, so this
test can run for a long time end-to-end and is therefore opt-in: set
``RUN_MANUAL_GOOGLE_SYSTEM_TESTS=1`` to run it.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import pytest

from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryStreamingBufferEmptySensor,
)

try:
    from airflow.sdk import TriggerRule, task
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.decorators import task  # type: ignore[no-redef,attr-defined]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_MANUAL_GOOGLE_SYSTEM_TESTS"),
    reason="Manual-only system test: set RUN_MANUAL_GOOGLE_SYSTEM_TESTS=1 to run.",
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "bigquery_streaming_buffer_sensor"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}".replace("-", "_")
TABLE_NAME = f"partitioned_table_{DAG_ID}_{ENV_ID}".replace("-", "_")

# DML on rows still in the streaming buffer is rejected by BigQuery, hence the
# sensor in the streaming-insert -> sensor -> DML chain below.
STREAMING_UPDATE_QUERY = f"UPDATE {DATASET_NAME}.{TABLE_NAME} SET value = 200 WHERE value = 100"
STREAMING_DELETE_QUERY = f"DELETE FROM {DATASET_NAME}.{TABLE_NAME} WHERE value = 200"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery", "sensors", "manual"],
    user_defined_macros={"DATASET": DATASET_NAME, "TABLE": TABLE_NAME},
    default_args={"project_id": PROJECT_ID},
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME, project_id=PROJECT_ID
    )

    create_table = BigQueryCreateTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        table_resource={
            "schema": {"fields": SCHEMA},
            "timePartitioning": {
                "type": "DAY",
                "field": "ds",
            },
        },
    )

    @task(task_id="streaming_insert")
    def streaming_insert(ds: str | None = None) -> None:
        hook = BigQueryHook()
        hook.insert_all(
            project_id=PROJECT_ID,
            dataset_id=DATASET_NAME,
            table_id=TABLE_NAME,
            rows=[{"value": 100, "ds": ds}],
            fail_on_error=True,
        )
        # BigQuery's streamingBuffer table metadata is eventually consistent: for
        # a few seconds after a streaming insert the row is in the buffer but
        # table.streaming_buffer is still None. Wait for the metadata to catch up
        # so check_streaming_buffer_empty does not falsely report "empty" before
        # the buffer is reported at all. Remove once the sensor handles this
        # itself; tracked at https://github.com/apache/airflow/issues/66963
        client = hook.get_client(project_id=PROJECT_ID)
        table_uri = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
        for _ in range(30):
            if client.get_table(table_uri).streaming_buffer is not None:
                return
            time.sleep(2)
        raise RuntimeError("BigQuery streaming buffer metadata did not appear within 60s")

    streaming_insert_task = streaming_insert()

    # [START howto_sensor_bigquery_streaming_buffer_empty]
    check_streaming_buffer_empty = BigQueryStreamingBufferEmptySensor(
        task_id="check_streaming_buffer_empty",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        poke_interval=30,
        timeout=5400,  # BigQuery flushes the streaming buffer within ~90 minutes
    )
    # [END howto_sensor_bigquery_streaming_buffer_empty]

    stream_update = BigQueryInsertJobOperator(
        task_id="stream_update",
        configuration={
            "query": {
                "query": STREAMING_UPDATE_QUERY,
                "useLegacySql": False,
            }
        },
    )

    # [START howto_sensor_bigquery_streaming_buffer_empty_deferred]
    check_streaming_buffer_empty_def = BigQueryStreamingBufferEmptySensor(
        task_id="check_streaming_buffer_empty_def",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        deferrable=True,
        poke_interval=30,
        timeout=5400,  # BigQuery flushes the streaming buffer within ~90 minutes
    )
    # [END howto_sensor_bigquery_streaming_buffer_empty_deferred]

    stream_delete = BigQueryInsertJobOperator(
        task_id="stream_delete",
        configuration={
            "query": {
                "query": STREAMING_DELETE_QUERY,
                "useLegacySql": False,
            }
        },
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        create_dataset
        >> create_table
        >> streaming_insert_task
        >> check_streaming_buffer_empty
        >> stream_update
        >> check_streaming_buffer_empty_def
        >> stream_delete
        >> delete_dataset
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
