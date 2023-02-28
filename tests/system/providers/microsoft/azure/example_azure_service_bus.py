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
from datetime import datetime, timedelta

import pytest

from airflow import DAG
from airflow.models.baseoperator import chain

try:
    from airflow.providers.microsoft.azure.operators.asb import (
        ASBReceiveSubscriptionMessageOperator,
        AzureServiceBusCreateQueueOperator,
        AzureServiceBusDeleteQueueOperator,
        AzureServiceBusReceiveMessageOperator,
        AzureServiceBusSendMessageOperator,
        AzureServiceBusSubscriptionCreateOperator,
        AzureServiceBusSubscriptionDeleteOperator,
        AzureServiceBusTopicCreateOperator,
        AzureServiceBusTopicDeleteOperator,
        AzureServiceBusUpdateSubscriptionOperator,
    )
except ImportError:
    pytest.skip("Azure Service Bus not available", allow_module_level=True)

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

CLIENT_ID = os.getenv("CLIENT_ID", "")
QUEUE_NAME = "sb_mgmt_queue_test"
MESSAGE = "Test Message"
MESSAGE_LIST = [MESSAGE + " " + str(n) for n in range(0, 10)]
TOPIC_NAME = "sb_mgmt_topic_test"
SUBSCRIPTION_NAME = "sb_mgmt_subscription"

with DAG(
    dag_id="example_azure_service_bus",
    start_date=datetime(2021, 8, 13),
    schedule=None,
    catchup=False,
    default_args={
        "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
        "azure_service_bus_conn_id": "azure_service_bus_default",
    },
    tags=["example", "Azure service bus"],
) as dag:
    # [START howto_operator_create_service_bus_queue]
    create_service_bus_queue = AzureServiceBusCreateQueueOperator(
        task_id="create_service_bus_queue",
        queue_name=QUEUE_NAME,
    )
    # [END howto_operator_create_service_bus_queue]

    # [START howto_operator_send_message_to_service_bus_queue]
    send_message_to_service_bus_queue = AzureServiceBusSendMessageOperator(
        task_id="send_message_to_service_bus_queue",
        message=MESSAGE,
        queue_name=QUEUE_NAME,
        batch=False,
    )
    # [END howto_operator_send_message_to_service_bus_queue]

    # [START howto_operator_send_list_message_to_service_bus_queue]
    send_list_message_to_service_bus_queue = AzureServiceBusSendMessageOperator(
        task_id="send_list_message_to_service_bus_queue",
        message=MESSAGE_LIST,
        queue_name=QUEUE_NAME,
        batch=False,
    )
    # [END howto_operator_send_list_message_to_service_bus_queue]

    # [START howto_operator_send_batch_message_to_service_bus_queue]
    send_batch_message_to_service_bus_queue = AzureServiceBusSendMessageOperator(
        task_id="send_batch_message_to_service_bus_queue",
        message=MESSAGE_LIST,
        queue_name=QUEUE_NAME,
        batch=True,
    )
    # [END howto_operator_send_batch_message_to_service_bus_queue]

    # [START howto_operator_receive_message_service_bus_queue]
    receive_message_service_bus_queue = AzureServiceBusReceiveMessageOperator(
        task_id="receive_message_service_bus_queue",
        queue_name=QUEUE_NAME,
        max_message_count=20,
        max_wait_time=5,
    )
    # [END howto_operator_receive_message_service_bus_queue]

    # [START howto_operator_create_service_bus_topic]
    create_service_bus_topic = AzureServiceBusTopicCreateOperator(
        task_id="create_service_bus_topic", topic_name=TOPIC_NAME
    )
    # [END howto_operator_create_service_bus_topic]

    # [START howto_operator_create_service_bus_subscription]
    create_service_bus_subscription = AzureServiceBusSubscriptionCreateOperator(
        task_id="create_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
    )
    # [END howto_operator_create_service_bus_subscription]

    # [START howto_operator_update_service_bus_subscription]
    update_service_bus_subscription = AzureServiceBusUpdateSubscriptionOperator(
        task_id="update_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
        max_delivery_count=5,
    )
    # [END howto_operator_update_service_bus_subscription]

    # [START howto_operator_receive_message_service_bus_subscription]
    receive_message_service_bus_subscription = ASBReceiveSubscriptionMessageOperator(
        task_id="receive_message_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
        max_message_count=10,
    )
    # [END howto_operator_receive_message_service_bus_subscription]

    # [START howto_operator_delete_service_bus_subscription]
    delete_service_bus_subscription = AzureServiceBusSubscriptionDeleteOperator(
        task_id="delete_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
        trigger_rule="all_done",
    )
    # [END howto_operator_delete_service_bus_subscription]

    # [START howto_operator_delete_service_bus_topic]
    delete_asb_topic = AzureServiceBusTopicDeleteOperator(
        task_id="delete_asb_topic",
        topic_name=TOPIC_NAME,
    )
    # [END howto_operator_delete_service_bus_topic]

    # [START howto_operator_delete_service_bus_queue]
    delete_service_bus_queue = AzureServiceBusDeleteQueueOperator(
        task_id="delete_service_bus_queue", queue_name=QUEUE_NAME, trigger_rule="all_done"
    )
    # [END howto_operator_delete_service_bus_queue]

    chain(
        create_service_bus_queue,
        create_service_bus_topic,
        create_service_bus_subscription,
        send_message_to_service_bus_queue,
        send_list_message_to_service_bus_queue,
        send_batch_message_to_service_bus_queue,
        receive_message_service_bus_queue,
        update_service_bus_subscription,
        receive_message_service_bus_subscription,
        delete_service_bus_subscription,
        delete_asb_topic,
        delete_service_bus_queue,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
