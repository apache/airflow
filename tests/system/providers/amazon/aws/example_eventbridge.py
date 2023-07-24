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

from airflow import DAG
from airflow.providers.amazon.aws.operators.eventbridge import EventBridgePutEventsOperator

DAG_ID = "example_eventbridge"
ENTRIES = [
    {
        "Detail": '{"event-name": "custom-event"}',
        "EventBusName": "custom-bus",
        "Source": "example.myapp",
        "DetailType": "Sample Custom Event",
    }
]

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:

    # [START howto_operator_eventbridge_put_events]

    put_events = EventBridgePutEventsOperator(task_id="put_events_task", entries=ENTRIES)

    # [END howto_operator_eventbridge_put_events]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
