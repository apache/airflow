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

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.microsoft.azure.operators.azure_service_bus_subscription import (
    ASBCreateSubscriptionOperator,
    ASBDeleteSubscriptionOperator,
    ASBReceiveSubscriptionMessageOperator,
    ASBUpdateSubscriptionOperator,
)

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "azure_service_bus_conn_id": "azure_service_bus_default",
}

CLIENT_ID = os.getenv("CLIENT_ID", "")
TOPIC_NAME = "sb_mgmt_topic_test"
SUBSCRIPTION_NAME = "sb_mgmt_subscription"

with DAG(
    dag_id="example_azure_service_bus_subscription",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "Azure service bus subscription"],
) as dag:
    # [START howto_operator_create_service_bus_subscription]
    create_service_bus_subscription = ASBCreateSubscriptionOperator(
        task_id="create_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
    )
    # [END howto_operator_create_service_bus_subscription]

    # [START howto_operator_update_service_bus_subscription]
    update_service_bus_subscription = ASBUpdateSubscriptionOperator(
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
    delete_service_bus_subscription = ASBDeleteSubscriptionOperator(
        task_id="delete_service_bus_subscription",
        topic_name=TOPIC_NAME,
        subscription_name=SUBSCRIPTION_NAME,
        trigger_rule="all_done",
    )
    # [END howto_operator_delete_service_bus_subscription]

    (
        create_service_bus_subscription
        >> update_service_bus_subscription
        >> receive_message_service_bus_subscription
        >> delete_service_bus_subscription
    )
