from typing import Sequence

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.service_bus import AzureServiceBusAdminClientHook, \
    ServiceBusMessageHook


class AzureServiceBusCreateQueueOperator(BaseOperator):
    """
    Creates a Azure ServiceBus queue under a ServiceBus Namespace by using
    ServiceBusAdministrationClient

    :param queue_name: The name of the queue. should be unique
    :param azure_service_bus_conn_id: connection Id
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """Creates Queue in Service Bus namespace, by connecting to Service Bus Admin client"""
        # Create the hook
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # create queue with name
        hook.create_queue(self.queue_name)


class AzureServiceBusSendMessageOperator(BaseOperator):
    """
    Send Message or batch message to the service bus queue

    :param queue_name: The name of the queue in Service Bus namespace
    :param message: message which need to be sent to the queue
    :param batch: Its boolean flag by default it is set to `False`, if the message needs to be sent
    as batch message it can be set to `True`
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name", "message")
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        message: str,
        batch: bool = False,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.batch = batch
        self.message = message
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """
        Sends Message to the specific queue in Service Bus namespace,
        by connecting to Service Bus  client

        """
        # Create the hook
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # send message
        hook.send_message(self.queue_name, self.message, self.batch)


class AzureServiceBusReceiveMessageOperator(BaseOperator):
    """
    Receive a batch of messages at once in a specified Queue name

    :param queue_name: The name of the queue in Service Bus namespace
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """
        Receive Message in specific queue in Service Bus namespace,
        by connecting to Service Bus client
        """
        # Create the hook
        hook = ServiceBusMessageHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # Receive message
        hook.receive_message(self.queue_name)


class AzureServiceBusDeleteQueueOperator(BaseOperator):
    """
    Deletes the Queue in the Azure ServiceBus namespace

    :param queue_name: The name of the queue in Service Bus namespace
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        queue_name: str,
        azure_service_bus_conn_id: str = 'azure_service_bus_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.azure_service_bus_conn_id = azure_service_bus_conn_id

    def execute(self, context: "Context") -> None:
        """Delete Queue in Service Bus namespace, by connecting to Service Bus Admin client"""
        # Create the hook
        hook = AzureServiceBusAdminClientHook(azure_service_bus_conn_id=self.azure_service_bus_conn_id)

        # delete queue with name
        hook.delete_queue(self.queue_name)
