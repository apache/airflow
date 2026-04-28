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
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import pendulum

from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, get_current_context, task

if TYPE_CHECKING:
    from airflow.sdk.execution_time.context import Context, TriggeringAssetEventsAccessor

KAFKA_CONFIG_ID = "kafka_default"
TOPICS = ["fizz_buzz"]
DLQ_TOPIC = "dlq"
RETRY_COUNT = 3
"""Airflow Kafka connection
AIRFLOW_CONN_KAFKA_DEFAULT='{
  "conn_type": "general",
  "extra": {
    "bootstrap.servers": "broker:29092",
    "group.id": "kafka_default_group",
    "security.protocol": "PLAINTEXT",
    "enable.auto.commit": false,
    "auto.offset.reset": "latest"
  }
}'
"""
"""Kafka Command to verify messages are being produced to the topic:

# Create Topic
/bin/kafka-topics --bootstrap-server broker:29092 --create --topic fizz_buzz
/bin/kafka-topics --bootstrap-server broker:29092 --create --topic dlq


# Get offsets for the topic to verify messages are being produced
/bin/kafka-get-offsets --bootstrap-server broker:29092 --topic fizz_buzz
/bin/kafka-get-offsets --bootstrap-server broker:29092 --topic dlq

# List consumer groups to verify our consumer group is being registered
/bin/kafka-consumer-groups --bootstrap-server broker:29092 --list

# Get current offsets for the consumer group to verify messages are being consumed
/bin/kafka-consumer-groups --bootstrap-server broker:29092 --describe --group group_1
"""

SAMPLE_ORDERS = [
    {"order_id": "ORD-1001", "customer": "alice", "item": "widget", "quantity": 2, "price": 9.99},
    {"order_id": "ORD-1002", "customer": "bob", "item": "gadget", "quantity": 1, "price": 24.50},
    {"order_id": "ORD-1003", "customer": "carol", "item": "widget", "quantity": 5, "price": 9.99},
    {"order_id": "ORD-1004", "customer": "dave", "item": "doohickey", "quantity": 3, "price": 14.75},
    {"order_id": "ORD-1005", "customer": "eve", "item": "thingamajig", "quantity": 1, "price": 39.00},
    {"order_id": "ORD-1006", "customer": "frank", "item": "widget", "quantity": 10, "price": 9.99},
    {"order_id": "ORD-1007", "customer": "grace", "item": "gadget", "quantity": 2, "price": 24.50},
    {"order_id": "ORD-1008", "customer": "heidi", "item": "doohickey", "quantity": 1, "price": 14.75},
]


def producer_function():
    for order in SAMPLE_ORDERS:
        yield (json.dumps(order["order_id"]), json.dumps(order))
    # produce a malformed message to demonstrate error handling
    yield ("malformed_message", "malformed_message")


def process_one_message(message: str):
    order = json.loads(message)
    total = order["quantity"] * order["price"]
    print(f"Order {order['order_id']}: {order['quantity']}x {order['item']} = ${total:.2f}")
    return order


def handle_dlq():
    context: Context = get_current_context()
    triggering_asset_events: TriggeringAssetEventsAccessor = context["triggering_asset_events"]
    for event in triggering_asset_events[kafka_cdc_asset]:
        print(f"Handling failed message from event: {event}")
        value = json.dumps(
            {
                "asset": event.asset.model_dump(mode="json"),
                "extra": event.extra,
            }
        )
        yield (json.dumps(event.asset.uri), value)


# Airflow 3 example
# Define a trigger that listens to an external message queue (Apache Kafka in this case)
trigger = MessageQueueTrigger(
    scheme="kafka",
    # the rest of the parameters are used by the trigger
    kafka_config_id=KAFKA_CONFIG_ID,
    topics=TOPICS,
    poll_interval=1,
    poll_timeout=1,
    commit_offset=True,
)

# Define an asset that watches for messages on the queue
kafka_cdc_asset = Asset("kafka_cdc_asset", watchers=[AssetWatcher(name="kafka_cdc", trigger=trigger)])


@dag(
    schedule=[kafka_cdc_asset],
    tags=["event-driven"],
)
def event_driven_consumer():

    @task(retries=RETRY_COUNT, retry_delay=timedelta(seconds=1))
    def process_message(**context) -> bool:
        # Extract the triggering asset events from the context
        triggering_asset_events: TriggeringAssetEventsAccessor = context["triggering_asset_events"]
        for event in triggering_asset_events[kafka_cdc_asset]:
            # Get the message from the TriggerEvent payload
            print(f"Asset event: {event}")
            process_one_message(cast("str", event.extra["payload"]))
        return True

    @task.short_circuit(trigger_rule="all_done")
    def should_handle_dlq(**context) -> bool:
        """Skip DLQ handling if processing succeeded."""
        # If process_message succeeded, it pushed True to XCom.
        # If it failed (exception after retries), no XCom was pushed -> None.
        upstream_result = context["ti"].xcom_pull(task_ids="process_message")
        return upstream_result is None

    result = process_message()
    dlq_check = should_handle_dlq()
    handle_dlq_task = ProduceToTopicOperator(
        kafka_config_id=KAFKA_CONFIG_ID,
        task_id="handle_dlq",
        topic=DLQ_TOPIC,
        producer_function=handle_dlq,
    )

    result >> dlq_check >> handle_dlq_task


event_driven_consumer()


@dag(
    description="Load Data to fizz_buzz topic",
    start_date=pendulum.datetime(2022, 11, 1),
    schedule=None,
    catchup=False,
    tags=["event-driven"],
)
def event_driven_producer():
    ProduceToTopicOperator(
        kafka_config_id=KAFKA_CONFIG_ID,
        task_id="produce_to_topic",
        topic=TOPICS[0],
        producer_function=producer_function,
    )


event_driven_producer()
