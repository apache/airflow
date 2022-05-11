import os
import uuid
from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.microsoft.azure.operators.azure_service_bus_queue import (
    AzureServiceBusCreateQueueOperator,
    AzureServiceBusDeleteQueueOperator,
    AzureServiceBusSendMessageOperator,
    AzureServiceBusReceiveMessageOperator
)

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "azure_service_bus_conn_id": "azure_service_bus_default",
}

CLIENT_ID = os.getenv("CLIENT_ID", "")
QUEUE_NAME = "sb_mgmt_queue_test"

with DAG(
        dag_id="example_azure_service_bus_queue",
        start_date=datetime(2021, 8, 13),
        schedule_interval=None,
        catchup=False,
        default_args=default_args,
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
        message="Test message",
        queue_name=QUEUE_NAME,
        batch=True,
    )
    # [END howto_operator_send_message_to_service_bus_queue]

    # [START howto_operator_send_message_to_service_bus_queue]
    receive_message_service_bus_queue = AzureServiceBusReceiveMessageOperator(
        task_id="receive_message_service_bus_queue",
        queue_name=QUEUE_NAME,
    )
    # [END howto_operator_send_message_to_service_bus_queue]

    # [START howto_operator_delete_service_bus_queue]
    delete_service_bus_queue = AzureServiceBusDeleteQueueOperator(
        task_id="delete_service_bus_queue",
        queue_name=QUEUE_NAME,
        trigger_rule="all_done"
    )
    # [END howto_operator_delete_service_bus_queue]

    (
            create_service_bus_queue
            >> send_message_to_service_bus_queue
            >> receive_message_service_bus_queue
            >> delete_service_bus_queue
    )
