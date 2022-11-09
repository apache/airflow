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

from unittest.mock import patch

import pytest
from zenpy.lib.api_objects import Ticket

from airflow.models import Connection
from airflow.providers.zendesk.hooks.zendesk import ZendeskHook
from airflow.utils import db


class TestZendeskHook:
    conn_id = "zendesk_conn_id_test"

    @pytest.fixture(autouse=True)
    def init_connection(self):
        db.merge_conn(
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
        assert self.hook._ZendeskHook__url == "https://yoursubdomain.zendesk.com"

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
