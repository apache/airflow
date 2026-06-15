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

from functools import cached_property
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

    Authentication modes (configured via Connection extras):

    - **API token** (recommended): Set ``token`` in the extra field to your Zendesk
      API token. The ``login`` field should be your email address.
    - **API token via password field**: Set ``use_token: true`` in extras and put
      the API token in the ``password`` field. Useful when managing secrets via
      environment variables. ``login`` should be your email address.
    - **OAuth token**: Set ``oauth_token`` in the extra field. ``login`` is not
      required for OAuth.
    - **Password** (deprecated): If none of the above extras are set, the
      ``password`` field is used for basic authentication. Zendesk has deprecated
      this method; prefer API token auth.

    Precedence order when multiple extras are set: ``use_token`` → ``token`` →
    ``oauth_token`` → password fallback.
    """

    conn_name_attr = "zendesk_conn_id"
    default_conn_name = "zendesk_default"
    conn_type = "zendesk"
    hook_name = "Zendesk"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Relabel fields for the Connection UI."""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "host": "Zendesk domain",
                "login": "Zendesk email",
                "password": "Password / API token",
            },
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Add custom widgets for the Connection UI."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from wtforms import BooleanField, StringField

        return {
            "use_token": BooleanField(
                "Use Token", description="If enabled, the password field is treated as an API token."
            ),
            "token": StringField(
                "API Token",
                widget=BS3PasswordFieldWidget(),
                description="Zendesk API token (alternative to password field).",
            ),
            "oauth_token": StringField(
                "OAuth Token",
                widget=BS3PasswordFieldWidget(),
                description="Zendesk OAuth token.",
            ),
        }

    def __init__(self, zendesk_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.zendesk_conn_id = zendesk_conn_id
        self.base_api: BaseApi | None = None
        self.__url: str = ""

    def _init_conn(self) -> tuple[Zenpy, str]:
        """
        Create the Zenpy Client for the Zendesk connection.

        Parses the host into ``domain`` and (optionally) ``subdomain`` for Zenpy.
        For example, ``yoursubdomain.zendesk.com`` produces
        ``domain="zendesk.com"`` and ``subdomain="yoursubdomain"``.

        Authentication kwargs are resolved from Connection extras according to
        the precedence documented on the class docstring.

        :return: (zenpy.Zenpy client, base URL string)
        :raises ValueError: if the host is missing or has an invalid format.
        """
        conn = self.get_connection(self.zendesk_conn_id)

        if not conn.host:
            raise ValueError(
                f"No host provided for connection '{self.zendesk_conn_id}'. "
                "Set the host to your Zendesk domain, e.g. 'yoursubdomain.zendesk.com'."
            )

        # Parse host into subdomain + domain.
        # Handle trailing dots and extract domain (last two parts) and subdomain (the rest).
        host = conn.host.strip("/")
        if host.endswith("."):
            host = host[:-1]

        parts = host.split(".")
        if len(parts) < 2:
            raise ValueError(
                f"Invalid host format '{conn.host}' for connection '{self.zendesk_conn_id}'. "
                "Expected a domain with at least one dot, e.g. 'yoursubdomain.zendesk.com'."
            )

        domain = ".".join(parts[-2:])
        subdomain: str | None = ".".join(parts[:-2]) if len(parts) > 2 else None
        url = f"https://{host}"

        extra = conn.extra_dejson
        kwargs: dict[str, Any] = {
            "domain": domain,
            "subdomain": subdomain,
        }

        if extra.get("use_token"):
            # Treat the password field as an API token.
            if not conn.login:
                raise ValueError(
                    f"No login provided for connection '{self.zendesk_conn_id}'. "
                    "The login field must be set to your Zendesk email address when using API token "
                    "authentication."
                )
            kwargs["email"] = conn.login
            kwargs["token"] = conn.password
        elif extra.get("token"):
            # API token stored directly in extras.
            if not conn.login:
                raise ValueError(
                    f"No login provided for connection '{self.zendesk_conn_id}'. "
                    "The login field must be set to your Zendesk email address when using API token "
                    "authentication."
                )
            kwargs["email"] = conn.login
            kwargs["token"] = extra["token"]
        elif extra.get("oauth_token"):
            # OAuth token stored in extras. email is NOT required.
            kwargs["oauth_token"] = extra["oauth_token"]
        else:
            # Legacy password-based auth (deprecated by Zendesk).
            if not conn.login:
                raise ValueError(
                    f"No login provided for connection '{self.zendesk_conn_id}'. "
                    "The login field must be set to your Zendesk email address when using password "
                    "authentication."
                )
            kwargs["email"] = conn.login
            kwargs["password"] = conn.password

        return Zenpy(**kwargs), url

    @cached_property
    def zenpy_client(self) -> Zenpy:
        """
        Get the underlying Zenpy client (cached property for backward compatibility).

        :return: zenpy.Zenpy client.
        """
        client, self.__url = self._init_conn()
        return client

    @property
    def _url(self) -> str:
        """Return the base URL, initializing the connection if needed."""
        if not self.__url:
            # Accessing zenpy_client triggers _init_conn which sets __url
            _ = self.zenpy_client
        return self.__url

    def get_conn(self) -> Zenpy:
        """
        Get the underlying Zenpy client (lazy-initialized).

        :return: zenpy.Zenpy client.
        """
        return self.zenpy_client

    @property
    def get(self) -> Any:
        """
        Expose the underlying Zenpy search/get method for backward compatibility.

        Used by system tests and legacy custom calls.
        """
        return self.get_conn().users._get

    def get_ticket(self, ticket_id: int) -> Ticket:
        """
        Retrieve ticket.

        :return: Ticket object retrieved.
        """
        return self.get_conn().tickets(id=ticket_id)

    def search_tickets(self, **kwargs) -> SearchResultGenerator:
        """
        Search tickets.

        :param kwargs: (optional) Search fields given to the zenpy search method.
        :return: SearchResultGenerator of Ticket objects.
        """
        return self.get_conn().search(type="ticket", **kwargs)

    def create_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> TicketAudit | JobStatus:
        """
        Create tickets.

        :param tickets: Ticket or List of Ticket to create.
        :param kwargs: (optional) Additional fields given to the zenpy create method.
        :return: A TicketAudit object containing information about the Ticket created.
            When sending bulk request, returns a JobStatus object.
        """
        return self.get_conn().tickets.create(tickets, **kwargs)

    def update_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> TicketAudit | JobStatus:
        """
        Update tickets.

        :param tickets: Updated Ticket or List of Ticket object to update.
        :param kwargs: (optional) Additional fields given to the zenpy update method.
        :return: A TicketAudit object containing information about the Ticket updated.
            When sending bulk request, returns a JobStatus object.
        """
        return self.get_conn().tickets.update(tickets, **kwargs)

    def delete_tickets(self, tickets: Ticket | list[Ticket], **kwargs) -> None:
        """
        Delete tickets, returns nothing on success and raises APIException on failure.

        :param tickets: Ticket or List of Ticket to delete.
        :param kwargs: (optional) Additional fields given to the zenpy delete method.
        :return:
        """
        return self.get_conn().tickets.delete(tickets, **kwargs)
