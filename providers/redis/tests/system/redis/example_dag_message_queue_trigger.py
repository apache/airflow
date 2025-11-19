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

# [START howto_trigger_message_queue]
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset, AssetWatcher

# Define a trigger that listens to an external message queue (Redis in this case)
trigger = MessageQueueTrigger(scheme="redis+pubsub", channels=["test"])

# Define an asset that watches for messages on the queue
asset = Asset("redis_queue_asset_1", watchers=[AssetWatcher(name="redis_watcher_1", trigger=trigger)])

with DAG(dag_id="example_redis_watcher_1", schedule=[asset]) as dag:
    EmptyOperator(task_id="task_1")
# [END howto_trigger_message_queue]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
