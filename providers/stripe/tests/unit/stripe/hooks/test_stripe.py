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

from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.stripe.hooks.stripe import StripeHook

API_KEY = "sk_test_abc123"
CUSTOMER_ID = "cus_test123"
SUBSCRIPTION_ID = "sub_test456"


class TestStripeHook:
    def _make_connection(self, password: str | None = API_KEY, extra: dict | None = None):
        import json

        return Connection(
            conn_id="stripe_default",
            conn_type="stripe",
            password=password,
            extra=json.dumps(extra or {}),
        )

    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_get_client_returns_stripe_client(self, mock_get_conn):
        mock_get_conn.return_value = self._make_connection()
        with mock.patch("airflow.providers.stripe.hooks.stripe.stripe.StripeClient") as mock_client_cls:
            hook = StripeHook()
            client = hook.get_client()
            mock_client_cls.assert_called_once_with(API_KEY)
            assert client is mock_client_cls.return_value

    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_get_client_with_stripe_version(self, mock_get_conn):
        import json

        mock_get_conn.return_value = Connection(
            conn_type="stripe",
            password=API_KEY,
            extra=json.dumps({"stripe_version": "2023-10-16"}),
        )
        with mock.patch("airflow.providers.stripe.hooks.stripe.stripe.StripeClient") as mock_client_cls:
            hook = StripeHook()
            hook.get_client()
            mock_client_cls.assert_called_once_with(API_KEY, stripe_version="2023-10-16")

    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_get_client_cached(self, mock_get_conn):
        mock_get_conn.return_value = self._make_connection()
        with mock.patch("airflow.providers.stripe.hooks.stripe.stripe.StripeClient") as mock_client_cls:
            hook = StripeHook()
            first = hook.get_client()
            second = hook.get_client()
            assert first is second
            mock_client_cls.assert_called_once()

    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_raises_when_no_api_key(self, mock_get_conn):
        mock_get_conn.return_value = self._make_connection(password=None)
        hook = StripeHook()
        with pytest.raises(AirflowException, match="No Stripe API key found"):
            hook.get_client()

    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_list_all_returns_paged_results(self, mock_get_conn):
        mock_get_conn.return_value = self._make_connection()

        fake_objects = [mock.MagicMock(id=f"cus_{i}") for i in range(3)]
        mock_list_result = mock.MagicMock()
        mock_list_result.auto_paging_iter.return_value = iter(fake_objects)

        mock_customers = mock.MagicMock()
        mock_customers.list.return_value = mock_list_result

        mock_client = mock.MagicMock()
        mock_client.customers = mock_customers

        with mock.patch.object(StripeHook, "get_client", return_value=mock_client):
            hook = StripeHook()
            results = hook.list_all("Customer", limit=100)

        mock_customers.list.assert_called_once_with({"limit": 100})
        assert results == fake_objects

    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_list_all_raises_for_unknown_resource(self, mock_get_conn):
        mock_get_conn.return_value = self._make_connection()

        mock_client = mock.MagicMock(spec=[])
        with mock.patch.object(StripeHook, "get_client", return_value=mock_client):
            hook = StripeHook()
            with pytest.raises(AirflowException, match="Stripe client has no resource"):
                hook.list_all("NonExistentResource")

    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_retrieve_returns_object(self, mock_get_conn):
        mock_get_conn.return_value = self._make_connection()

        fake_customer = mock.MagicMock(id=CUSTOMER_ID)
        mock_customers = mock.MagicMock()
        mock_customers.retrieve.return_value = fake_customer

        mock_client = mock.MagicMock()
        mock_client.customers = mock_customers

        with mock.patch.object(StripeHook, "get_client", return_value=mock_client):
            hook = StripeHook()
            result = hook.retrieve("Customer", CUSTOMER_ID)

        mock_customers.retrieve.assert_called_once_with(CUSTOMER_ID, {})
        assert result is fake_customer

    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_retrieve_raises_for_unknown_resource(self, mock_get_conn):
        mock_get_conn.return_value = self._make_connection()

        mock_client = mock.MagicMock(spec=[])
        with mock.patch.object(StripeHook, "get_client", return_value=mock_client):
            hook = StripeHook()
            with pytest.raises(AirflowException, match="Stripe client has no resource"):
                hook.retrieve("NonExistentResource", "id_123")

    @pytest.mark.parametrize(
        ("resource", "expected_attr"),
        [
            ("Customer", "customers"),
            ("Subscription", "subscriptions"),
            ("Invoice", "invoices"),
            ("Event", "events"),
        ],
    )
    @mock.patch("airflow.providers.stripe.hooks.stripe.StripeHook.get_connection")
    def test_resource_name_lowercasing(self, mock_get_conn, resource, expected_attr):
        mock_get_conn.return_value = self._make_connection()

        mock_resource_client = mock.MagicMock()
        mock_resource_client.list.return_value = mock.MagicMock(auto_paging_iter=lambda: iter([]))

        mock_client = mock.MagicMock()
        setattr(mock_client, expected_attr, mock_resource_client)

        with mock.patch.object(StripeHook, "get_client", return_value=mock_client):
            hook = StripeHook()
            hook.list_all(resource)

        mock_resource_client.list.assert_called_once()
