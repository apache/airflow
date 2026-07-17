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
from unittest.mock import patch

import pytest
from zenpy.lib.api_objects import Ticket

from airflow.models import Connection
from airflow.providers.zendesk.hooks.zendesk import ZendeskHook


class TestZendeskHook:
    conn_id = "zendesk_conn_id_test"

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=self.conn_id,
                conn_type="zendesk",
                host="yoursubdomain.zendesk.com",
                login="user@gmail.com",
                password="eb243592-faa2-4ba2-a551q-1afdf565c889",
            )
        )
        self.hook = ZendeskHook(zendesk_conn_id=self.conn_id)

    def test_hook_init_and_get_conn(self):
        # Verify config of zenpy APIs
        zenpy_client = self.hook.get_conn()
        assert zenpy_client.users.subdomain == "yoursubdomain"
        assert zenpy_client.users.domain == "zendesk.com"
        assert zenpy_client.users.session.auth == ("user@gmail.com", "eb243592-faa2-4ba2-a551q-1afdf565c889")
        assert not zenpy_client.cache.disabled
        assert self.hook._url == "https://yoursubdomain.zendesk.com"

    def test_get_conn_is_lazy_and_cached(self):
        """get_conn() should return the same client instance on repeated calls."""
        client1 = self.hook.get_conn()
        client2 = self.hook.get_conn()
        assert client1 is client2

    # ------------------------------------------------------------------
    # Authentication mode tests
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("host", "expected_subdomain", "expected_domain"),
        [
            ("yoursubdomain.zendesk.com", "yoursubdomain", "zendesk.com"),
            ("zendesk.com", None, "zendesk.com"),
            ("sub.company.zendesk.com", "sub.company", "zendesk.com"),
        ],
    )
    def test_host_parsing(self, create_connection_without_db, host, expected_subdomain, expected_domain):
        conn_id = f"zendesk_host_test_{host.replace('.', '_')}"
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="zendesk",
                host=host,
                login="user@gmail.com",
                password="secret",
            )
        )
        hook = ZendeskHook(zendesk_conn_id=conn_id)
        client = hook.get_conn()
        assert client.users.subdomain == expected_subdomain
        assert client.users.domain == expected_domain

    def test_invalid_host_no_dot_raises_value_error(self, create_connection_without_db):
        """A host with no dot (e.g. just 'zendesk') must raise ValueError."""
        conn_id = "zendesk_bad_host"
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="zendesk",
                host="zendesk",
                login="user@gmail.com",
                password="secret",
            )
        )
        hook = ZendeskHook(zendesk_conn_id=conn_id)
        with pytest.raises(ValueError, match="Invalid host format"):
            hook.get_conn()

    def test_missing_host_raises_value_error(self, create_connection_without_db):
        """A connection with no host must raise ValueError."""
        conn_id = "zendesk_no_host"
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="zendesk",
                host=None,
                login="user@gmail.com",
                password="secret",
            )
        )
        hook = ZendeskHook(zendesk_conn_id=conn_id)
        with pytest.raises(ValueError, match="No host provided"):
            hook.get_conn()

    def test_auth_use_token_flag(self, create_connection_without_db):
        """use_token=true in extras should pass conn.password as the API token."""
        conn_id = "zendesk_use_token"
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="zendesk",
                host="yoursubdomain.zendesk.com",
                login="user@gmail.com",
                password="my-api-token",
                extra=json.dumps({"use_token": True}),
            )
        )
        hook = ZendeskHook(zendesk_conn_id=conn_id)
        client = hook.get_conn()
        # Zenpy encodes token auth as "<email>/token:<token>"
        assert client.users.session.auth == ("user@gmail.com/token", "my-api-token")

    def test_auth_token_in_extra(self, create_connection_without_db):
        """A 'token' key in extras should be used as the API token directly."""
        conn_id = "zendesk_token_extra"
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="zendesk",
                host="yoursubdomain.zendesk.com",
                login="user@gmail.com",
                extra=json.dumps({"token": "extra-api-token"}),
            )
        )
        hook = ZendeskHook(zendesk_conn_id=conn_id)
        client = hook.get_conn()
        assert client.users.session.auth == ("user@gmail.com/token", "extra-api-token")

    def test_auth_oauth_token_in_extra(self, create_connection_without_db):
        """An 'oauth_token' key in extras should configure OAuth authentication."""
        conn_id = "zendesk_oauth"
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="zendesk",
                host="yoursubdomain.zendesk.com",
                login="user@gmail.com",
                extra=json.dumps({"oauth_token": "my-oauth-token"}),
            )
        )
        hook = ZendeskHook(zendesk_conn_id=conn_id)
        client = hook.get_conn()
        # Zenpy sets a Bearer token header for OAuth
        assert "Authorization" in client.users.session.headers
        assert client.users.session.headers["Authorization"] == "Bearer my-oauth-token"

    def test_auth_precedence_use_token_over_token_extra(self, create_connection_without_db):
        """use_token flag takes precedence over a token key in extras."""
        conn_id = "zendesk_precedence"
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="zendesk",
                host="yoursubdomain.zendesk.com",
                login="user@gmail.com",
                password="password-field-token",
                extra=json.dumps({"use_token": True, "token": "should-be-ignored"}),
            )
        )
        hook = ZendeskHook(zendesk_conn_id=conn_id)
        client = hook.get_conn()
        # use_token takes priority: password field is the token
        assert client.users.session.auth == ("user@gmail.com/token", "password-field-token")

    # ------------------------------------------------------------------
    # Ticket operation tests
    # ------------------------------------------------------------------

    def test_get_ticket(self):
        zenpy_client = self.hook.get_conn()
        with patch.object(zenpy_client, "tickets") as tickets_mock:
            self.hook.get_ticket(ticket_id=1)
            tickets_mock.assert_called_once_with(id=1)

    def test_search_tickets(self):
        zenpy_client = self.hook.get_conn()
        with patch.object(zenpy_client, "search") as search_mock:
            self.hook.search_tickets(status="open", sort_order="desc")
            search_mock.assert_called_once_with(type="ticket", status="open", sort_order="desc")

    def test_create_tickets(self):
        zenpy_client = self.hook.get_conn()
        ticket = Ticket(subject="This is a test ticket to create")
        with patch.object(zenpy_client.tickets, "create") as search_mock:
            self.hook.create_tickets(ticket, extra_parameter="extra_parameter")
            search_mock.assert_called_once_with(ticket, extra_parameter="extra_parameter")

    def test_update_tickets(self):
        zenpy_client = self.hook.get_conn()
        ticket = Ticket(subject="This is a test ticket to update")
        with patch.object(zenpy_client.tickets, "update") as search_mock:
            self.hook.update_tickets(ticket, extra_parameter="extra_parameter")
            search_mock.assert_called_once_with(ticket, extra_parameter="extra_parameter")

    def test_delete_tickets(self):
        zenpy_client = self.hook.get_conn()
        ticket = Ticket(subject="This is a test ticket to delete")
        with patch.object(zenpy_client.tickets, "delete") as search_mock:
            self.hook.delete_tickets(ticket, extra_parameter="extra_parameter")
            search_mock.assert_called_once_with(ticket, extra_parameter="extra_parameter")

    def test_auth_oauth_token_no_login(self, create_connection_without_db):
        """OAuth authentication should not require a login/email."""
        conn_id = "zendesk_oauth_no_login"
        create_connection_without_db(
            Connection(
                conn_id=conn_id,
                conn_type="zendesk",
                host="yoursubdomain.zendesk.com",
                login=None,
                extra=json.dumps({"oauth_token": "my-oauth-token"}),
            )
        )
        hook = ZendeskHook(zendesk_conn_id=conn_id)
        client = hook.get_conn()
        assert "Authorization" in client.users.session.headers
        assert client.users.session.headers["Authorization"] == "Bearer my-oauth-token"
        # email/login should not be present in kwargs passed to Zenpy
        assert not hasattr(client.users, "email") or client.users.email is None
