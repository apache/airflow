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
Example Airflow DAG that demonstrates using Azure Service Bus with MessageQueueTrigger
and Asset Watchers for event-driven workflows.

This example shows how to create a DAG that triggers when messages arrive in an
Azure Service Bus queue using Asset Watchers.

Prerequisites
-------------

Before running this example, ensure you have:

1. An Azure Service Bus namespace
2. A queue created in the namespace (e.g., ``my-queue``)
3. An Airflow connection configured with ID ``azure_service_bus_default``

How to test
-----------

1. Ensure the Azure Service Bus queue exists (see Prerequisites above)
2. Send a message to the queue to trigger the DAG
3. The DAG will be triggered automatically when the message arrives
"""

from __future__ import annotations

# [START howto_trigger_asb_message_queue]
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset, AssetWatcher

# Define a trigger that listens to an Azure Service Bus queue
trigger = MessageQueueTrigger(
    scheme="azure+servicebus",
    queues=["my-queue"],
    azure_service_bus_conn_id="azure_service_bus_default",
    poll_interval=60,
)

# Define an asset that watches for messages on the Azure Service Bus queue
asset = Asset(
    "event_schedule_asb_asset_1",
    watchers=[AssetWatcher(name="event_schedule_asb_watcher_1", trigger=trigger)],
)

with DAG(
    dag_id="example_event_schedule_asb",
    schedule=[asset],
) as dag:
    process_message_task = EmptyOperator(task_id="process_asb_message")
# [END howto_trigger_asb_message_queue]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
