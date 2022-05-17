from typing import Dict, Any

from airflow.exceptions import AirflowBadRequest, AirflowException
from airflow.hooks.base import BaseHook
from azure.servicebus.management import ServiceBusAdministrationClient
from azure.servicebus.management import QueueProperties
from azure.servicebus import ServiceBusClient, ServiceBusMessage


class AzureServiceBusHook(BaseHook):
    conn_name_attr = 'azure_service_bus_conn_id'
    default_conn_name = 'azure_service_bus_default'
    conn_type = 'azure_service_bus'
    hook_name = 'Azure ServiceBus'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__azure_service_bus__connection_string": StringField(
                lazy_gettext('Service Bus Connection String'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema', 'port', 'host', 'extra'],
            "relabeling": {
                'login': 'Client ID',
                'password': 'Secret',
            },
            "placeholders": {
                'login': 'Client ID (Optional)',
                'password': 'Client Secret (Optional)',
                'extra__azure_service_bus__connection_string': 'Service Bus Connection String',
            },
        }

    def __init__(self, azure_service_bus_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_service_bus_conn_id
        self._conn = None
        self.connection_string = None

    def get_conn(self):
        return None


class AzureServiceBusAdminClientHook(AzureServiceBusHook):

    def get_conn(self) -> ServiceBusAdministrationClient:
        """Create and returns ServiceBusAdministration by using the connection string in connection details"""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson

        self.connection_string = extras.get('connection_string') or extras.get(
            'extra__azure_service_bus__connection_string'
        )
        return ServiceBusAdministrationClient.from_connection_string(self.connection_string)

    def create_queue(self, queue_name: str) -> QueueProperties:
        """
        Create Queue by connecting to service Bus Admin client return the QueueProperties

        :param queue_name: The name of the queue or a QueueProperties with name.
        """
        if queue_name is None:
            raise AirflowBadRequest("Queue name cannot be None.")

        with self.get_conn() as service_mgmt_conn:
            queue = service_mgmt_conn.create_queue(queue_name,
                                                   max_delivery_count=10,
                                                   dead_lettering_on_message_expiration=True)
            return queue.name

    def delete_queue(self, queue_name: str) -> None:
        """
        Delete the queue by queue_name in service bus namespace

        :param queue_name: The name of the queue or a QueueProperties with name.
        """
        if queue_name is None:
            raise AirflowBadRequest("Queue name cannot be None.")

        with self.get_conn() as service_mgmt_conn:
            service_mgmt_conn.delete_queue(queue_name)


class ServiceBusMessageHook(AzureServiceBusHook):
    """ Sending message(s) to a Service Bus Queue. By using ServiceBusClient"""

    def get_conn(self) -> ServiceBusClient:
        """Create and returns ServiceBusClient by using the connection string in connection details"""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson

        self.connection_string = extras.get('connection_string') or extras.get(
            'extra__azure_service_bus__connection_string'
        )

        return ServiceBusClient.from_connection_string(conn_str=self.connection_string, logging_enable=True)

    def send_message(self, queue_name: str, message: str, batch_message_flag: bool = False) -> str:
        """
        Sends single message to  a Service Bus Queue

        :param queue_name: The name of the queue or a QueueProperties with name.
        :param message: Message which needs to be sent to the queue.
        :param batch_message_flag: bool flag, can be set to True if message needs to be sent as batch message.
        """
        if queue_name is None:
            raise AirflowBadRequest("Queue name cannot be None.")

        msg = ServiceBusMessage(message)
        service_bus_client = self.get_conn()
        with service_bus_client:
            sender = service_bus_client.get_queue_sender(queue_name=queue_name)
            with sender:
                if batch_message_flag:
                    batch_message = sender.create_message_batch()
                    try:
                        batch_message.add_message(msg)
                        msg = batch_message
                    except ValueError:
                        # ServiceBusMessageBatch object reaches max_size.
                        # New ServiceBusMessageBatch object can be created here to send more data.
                        raise AirflowException("ServiceBusMessageBatch object reaches max_size.")
                sender.send_messages(msg)
        return "Message sent"

    def receive_message(self, queue_name):
        """
        Receive a batch of messages at once in a specified Queue name

        :param queue_name: The name of the queue name or a QueueProperties with name.
        """
        if queue_name is None:
            raise AirflowBadRequest("Queue name cannot be None.")

        service_bus_client = self.get_conn()
        with service_bus_client:
            receiver = service_bus_client.get_queue_receiver(queue_name=queue_name)
            with receiver:
                received_msgs = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                for msg in received_msgs:
                    receiver.complete_message(msg)
