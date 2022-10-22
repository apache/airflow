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
from unittest.mock import PropertyMock

import pytest

from airflow.providers.google.ads.hooks.ads import GoogleAdsHook

API_VERSION = "api_version"
ADS_CLIENT = {"key": "value"}
SECRET = "secret"
EXTRAS = {
    "keyfile_dict": SECRET,
    "google_ads_client": ADS_CLIENT,
}


@pytest.fixture()
def mock_hook():
    with mock.patch("airflow.hooks.base.BaseHook.get_connection") as conn:
        hook = GoogleAdsHook(api_version=API_VERSION)
        conn.return_value.extra_dejson = EXTRAS
        yield hook


class TestGoogleAdsHook:
    @mock.patch("airflow.providers.google.ads.hooks.ads.GoogleAdsClient")
    def test_get_customer_service(self, mock_client, mock_hook):
        mock_hook._get_customer_service()
        client = mock_client.load_from_dict
        client.assert_called_once_with(mock_hook.google_ads_config)
        client.return_value.get_service.assert_called_once_with("CustomerService", version=API_VERSION)

    @mock.patch("airflow.providers.google.ads.hooks.ads.GoogleAdsClient")
    def test_get_service(self, mock_client, mock_hook):
        mock_hook._get_service()
        client = mock_client.load_from_dict
        client.assert_called_once_with(mock_hook.google_ads_config)
        client.return_value.get_service.assert_called_once_with("GoogleAdsService", version=API_VERSION)

    @mock.patch("airflow.providers.google.ads.hooks.ads.GoogleAdsClient")
    def test_search(self, mock_client, mock_hook):
        service = mock_client.load_from_dict.return_value.get_service.return_value
        mock_client.load_from_dict.return_value.get_type.side_effect = [PropertyMock(), PropertyMock()]
        client_ids = ["1", "2"]
        rows = ["row1", "row2"]
        service.search.side_effects = rows

        # Here we mock _extract_rows to assert calls and
        # avoid additional __iter__ calls
        mock_hook._extract_rows = list
        query = "QUERY"
        mock_hook.search(client_ids=client_ids, query=query, page_size=2)
        for i, client_id in enumerate(client_ids):
            name, args, kwargs = service.search.mock_calls[i]
            assert kwargs['request'].customer_id == client_id
            assert kwargs['request'].query == query
            assert kwargs['request'].page_size == 2

    def test_extract_rows(self, mock_hook):
        iterators = [[1, 2, 3], [4, 5, 6]]
        assert mock_hook._extract_rows(iterators) == sum(iterators, [])

    @mock.patch("airflow.providers.google.ads.hooks.ads.GoogleAdsClient")
    def test_list_accessible_customers(self, mock_client, mock_hook):
        accounts = ["a", "b", "c"]
        service = mock_client.load_from_dict.return_value.get_service.return_value
        service.list_accessible_customers.return_value = mock.MagicMock(resource_names=accounts)

        result = mock_hook.list_accessible_customers()
        service.list_accessible_customers.assert_called_once_with()
        assert accounts == result
