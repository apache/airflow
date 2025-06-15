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

import json

# [START howto_trigger_message_queue]
from airflow.providers.apache.kafka.triggers.msg_queue import KafkaMessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset, AssetWatcher


def apply_function(message):
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return True


# Define a trigger that listens to an Apache Kafka message queue
trigger = KafkaMessageQueueTrigger(
    topics=["test"],
    apply_function="example_dag_kafka_message_queue_trigger.apply_function",
    kafka_config_id="kafka_default",
    apply_function_args=None,
    apply_function_kwargs=None,
    poll_timeout=1,
    poll_interval=5,
)

# Define an asset that watches for messages on the queue
asset = Asset("kafka_queue_asset_1", watchers=[AssetWatcher(name="kafka_watcher_1", trigger=trigger)])

with DAG(dag_id="example_kafka_watcher_1", schedule=[asset]) as dag:
    EmptyOperator(task_id="task")
# [END howto_trigger_message_queue]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
