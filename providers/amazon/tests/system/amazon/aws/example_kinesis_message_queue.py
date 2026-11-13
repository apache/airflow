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
Example Dag demonstrating event-driven scheduling with Amazon Kinesis Data Streams.

NOTE: This file serves as an example Dag and reference for AssetWatcher configuration.
It is NOT an automated end-to-end integration test: running this file directly (e.g. via
pytest system-test harness) initiates a manual DagRun where `triggering_asset_events`
is empty. Validating the complete event-driven chain requires a running Airflow triggerer,
an active Kinesis stream, and external records producing AssetEvents.

Pre-requisites:
1. An active Amazon Kinesis Data Stream must exist and be accessible by the configured AWS connection.
2. The Airflow triggerer must be running with the ``common.messaging`` provider installed.
3. This is an event-driven Dag triggered by an AssetWatcher; it does not produce records to itself.
"""

from __future__ import annotations

import base64
import os
from datetime import datetime

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, task

STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "airflow-kinesis-example-stream")
AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# [START howto_trigger_kinesis_message_queue]
trigger = MessageQueueTrigger(
    scheme="kinesis",
    stream_name=STREAM_NAME,
    aws_conn_id=AWS_CONN_ID,
    region_name=AWS_REGION,
    shard_iterator_type="TRIM_HORIZON",
)

kinesis_asset = Asset(
    f"kinesis://{STREAM_NAME}",
    watchers=[AssetWatcher(name="kinesis_stream_watcher", trigger=trigger)],
)


@task
def process_kinesis_records(**context) -> None:
    """Process and decode incoming records triggered from the Amazon Kinesis stream."""
    events = context["triggering_asset_events"].get(kinesis_asset, [])
    if not events:
        print(
            "No triggering asset events found for this run. "
            "When executed manually or via test runners without an active watcher, "
            "no Kinesis records are delivered. In an event-driven environment, "
            "the Airflow triggerer emits an AssetEvent containing Kinesis records."
        )
        return

    for event in events:
        message_batch = event.extra.get("payload", {}).get("message_batch", [])
        for record in message_batch:
            raw_data = base64.b64decode(record["Data"]).decode("utf-8")
            print(
                f"Received record: ShardId={record['ShardId']}, "
                f"SequenceNumber={record['SequenceNumber']}, "
                f"Data={raw_data}"
            )


with DAG(
    dag_id="example_kinesis_message_queue",
    schedule=[kinesis_asset],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "kinesis", "message_queue"],
) as dag:
    process_kinesis_records()
# [END howto_trigger_kinesis_message_queue]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
