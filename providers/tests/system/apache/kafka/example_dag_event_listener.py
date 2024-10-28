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

# listener_dag_function.py
from __future__ import annotations

import json

from pendulum import datetime

from airflow import DAG

# Connections needed for this example dag to finish
from airflow.models import Connection

# This is just for setting up connections in the demo - you should use standard
# methods for setting these connections in production
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor
from airflow.utils import db


def load_connections():
    db.merge_conn(
        Connection(
            conn_id="fizz_buzz_1",
            conn_type="kafka",
            extra=json.dumps(
                {"socket.timeout.ms": 10, "bootstrap.servers": "broker:29092"}
            ),
        )
    )

    db.merge_conn(
        Connection(
            conn_id="fizz_buzz_2",
            conn_type="kafka",
            extra=json.dumps(
                {
                    "bootstrap.servers": "broker:29092",
                    "group.id": "fizz_buzz",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "beginning",
                }
            ),
        )
    )


def _producer_function():
    for i in range(50):
        yield (json.dumps(i), json.dumps(i + 1))


with DAG(
    dag_id="fizzbuzz-load-topic",
    description="Load Data to fizz_buzz topic",
    start_date=datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
    tags=["fizz-buzz"],
) as dag:
    t0 = PythonOperator(task_id="load_connections", python_callable=load_connections)

    t1 = ProduceToTopicOperator(
        kafka_config_id="fizz_buzz_1",
        task_id="produce_to_topic",
        topic="fizz_buzz",
        producer_function=_producer_function,
    )

with DAG(
    dag_id="fizzbuzz-listener-dag",
    description="listen for messages with mod 3 and mod 5 are zero",
    start_date=datetime(2022, 11, 1),
    catchup=False,
    tags=["fizz", "buzz"],
):

    def await_function(message):
        val = json.loads(message.value())
        print(f"Value in message is {val}")
        if val % 3 == 0:
            return val
        if val % 5 == 0:
            return val

    def wait_for_event(message, **context):
        if message % 15 == 0:
            return f"encountered {message}!"
        else:
            if message % 3 == 0:
                print(f"encountered {message} FIZZ !")
            if message % 5 == 0:
                print(f"encountered {message} BUZZ !")

    # [START howto_sensor_await_message_trigger_function]
    listen_for_message = AwaitMessageTriggerFunctionSensor(
        kafka_config_id="fizz_buzz_2",
        task_id="listen_for_message",
        topics=["fizz_buzz"],
        apply_function="example_dag_event_listener.await_function",
        event_triggered_function=wait_for_event,
    )
    # [END howto_sensor_await_message_trigger_function]

    t0 >> t1


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
