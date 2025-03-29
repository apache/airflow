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

from airflow.models.dag import DAG
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset, AssetWatcher

# [START howto_trigger_message_queue]
trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/0123456789/my-queue")
asset = Asset("sqs_asset", watchers=[AssetWatcher(name="sqs_asset_watcher", trigger=trigger)])

with DAG(
    dag_id="example_msgq_watcher",
    schedule=[asset],
    catchup=False,
) as dag:
    EmptyOperator(task_id="task")
# [END howto_trigger_message_queue]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
