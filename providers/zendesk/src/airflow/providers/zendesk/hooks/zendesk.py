#
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

from typing import TYPE_CHECKING, Any

from zenpy import Zenpy

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from zenpy.lib.api import BaseApi
    from zenpy.lib.api_objects import JobStatus, Ticket, TicketAudit
    from zenpy.lib.generator import SearchResultGenerator


class ZendeskHook(BaseHook):
    """
    Interact with Zendesk. This hook uses the Zendesk conn_id.

    :param zendesk_conn_id: The Airflow connection used for Zendesk credentials.
    """

    conn_name_attr = "zendesk_conn_id"
    default_conn_name = "zendesk_default"
    conn_type = "zendesk"
    hook_name = "Zendesk"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {"host": "Zendesk domain", "login": "Zendesk email"},
        }

    def __init__(self, zendesk_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.zendesk_conn_id = zendesk_conn_id
        self.base_api: BaseApi | None = None
        zenpy_client, url = self._init_conn()
        self.zenpy_client = zenpy_client
        self.__url = url
        self.get = self.zenpy_client.users._get

    def _init_conn(self) -> tuple[Zenpy, str]:
        """
        Create the Zenpy Client for our Zendesk connection.

        :return: zenpy.Zenpy client and the url for the API.
        """
        conn = self.get_connection(self.zendesk_conn_id)
        domain = ""
        url = ""
        subdomain: str | None = None
        if conn.host:
            url = "https://" + conn.host
            domain = conn.host
            if conn.host.count(".") >= 2:
                dot_splitted_string = conn.host.rsplit(".", 2)
                subdomain = dot_splitted_string[0]
                domain = ".".join(dot_splitted_string[1:])
        return Zenpy(domain=domain, subdomain=subdomain, email=conn.login, password=conn.password), url

    def get_conn(self) -> Zenpy:
        """
        Get the underlying Zenpy client.

        :return: zenpy.Zenpy client.
        """
        return self.zenpy_client

    def get_ticket(self, ticket_id: int) -> Ticket:
        """
        Retrieve ticket.

        :return: Ticket object retrieved.
        """
        return self.zenpy_client.tickets(id=ticket_id)

    def search_tickets(self, **kwargs) -> SearchResultGenerator:
        """
        Search tickets.

        :param kwargs: (optional) Search fields given to the zenpy search method.
        :return: SearchResultGenerator of Ticket objects.
        """
        return self.zenpy_client.search(type="ticket", **kwargs)

    def create_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> TicketAudit | JobStatus:
        """
        Create tickets.

        :param tickets: Ticket or List of Ticket to create.
        :param kwargs: (optional) Additional fields given to the zenpy create method.
        :return: A TicketAudit object containing information about the Ticket created.
            When sending bulk request, returns a JobStatus object.
        """
        return self.zenpy_client.tickets.create(tickets, **kwargs)

    def update_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> TicketAudit | JobStatus:
        """
        Update tickets.

        :param tickets: Updated Ticket or List of Ticket object to update.
        :param kwargs: (optional) Additional fields given to the zenpy update method.
        :return: A TicketAudit object containing information about the Ticket updated.
            When sending bulk request, returns a JobStatus object.
        """
        return self.zenpy_client.tickets.update(tickets, **kwargs)

    def delete_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> None:
        """
        Delete tickets, returns nothing on success and raises APIException on failure.

        :param tickets: Ticket or List of Ticket to delete.
        :param kwargs: (optional) Additional fields given to the zenpy delete method.
        :return:
        """
        return self.zenpy_client.tickets.delete(tickets, **kwargs)
