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

from typing import Any
from unittest import mock

import pytest

from airflow.providers.microsoft.azure.utils import (
    AzureIdentityCredentialAdapter,
    add_managed_identity_connection_widgets,
    get_async_default_azure_credential,
    get_field,
    # _get_default_azure_credential
    get_sync_default_azure_credential,
    parse_blob_account_url,
)

MODULE = "airflow.providers.microsoft.azure.utils"


def test_get_field_warns_on_dupe():
    with pytest.warns(UserWarning, match="Using value for `this_param`"):
        value = get_field(
            conn_id="my_conn",
            conn_type="this_type",
            extras=dict(extra__this_type__this_param="prefixed", this_param="non-prefixed"),
            field_name="this_param",
        )
    assert value == "non-prefixed"


@pytest.mark.parametrize(
    "input, expected",
    [
        (dict(this_param="non-prefixed"), "non-prefixed"),
        (dict(this_param=None), None),
        (dict(extra__this_type__this_param="prefixed"), "prefixed"),
        (dict(extra__this_type__this_param=""), None),
        (dict(extra__this_type__this_param=None), None),
        (dict(extra__this_type__this_param="prefixed", this_param="non-prefixed"), "non-prefixed"),
        (dict(extra__this_type__this_param="prefixed", this_param=""), None),
        (dict(extra__this_type__this_param="prefixed", this_param=0), 0),
        (dict(extra__this_type__this_param="prefixed", this_param=False), False),
        (dict(extra__this_type__this_param="prefixed", this_param=" "), " "),
    ],
)
def test_get_field_non_prefixed(input, expected):
    value = get_field(
        conn_id="my_conn",
        conn_type="this_type",
        extras=input,
        field_name="this_param",
    )
    assert value == expected


def test_add_managed_identity_connection_widgets():
    class FakeHook:
        @classmethod
        @add_managed_identity_connection_widgets
        def test_class_method(cls) -> dict[str, Any]:
            return {"foo": "bar"}

    widgets = FakeHook.test_class_method()

    assert "managed_identity_client_id" in widgets
    assert "workload_identity_tenant_id" in widgets
    assert widgets["foo"] == "bar"


@mock.patch(f"{MODULE}.DefaultAzureCredential")
def test_get_sync_default_azure_credential(mock_default_azure_credential):
    get_sync_default_azure_credential()

    assert mock_default_azure_credential.called


@mock.patch(f"{MODULE}.AsyncDefaultAzureCredential")
def test_get_async_default_azure_credential(mock_default_azure_credential):
    get_async_default_azure_credential()
    assert mock_default_azure_credential.called


class TestAzureIdentityCredentialAdapter:
    @mock.patch(f"{MODULE}.PipelineRequest")
    @mock.patch(f"{MODULE}.BearerTokenCredentialPolicy")
    @mock.patch(f"{MODULE}.DefaultAzureCredential")
    def test_signed_session(self, mock_default_azure_credential, mock_policy, mock_request):
        mock_request.return_value.http_request.headers = {"Authorization": "Bearer token"}

        adapter = AzureIdentityCredentialAdapter()
        mock_default_azure_credential.assert_called_once()
        mock_policy.assert_called_once()

        adapter.signed_session()
        assert adapter.token == {"access_token": "token"}

    @mock.patch(f"{MODULE}.PipelineRequest")
    @mock.patch(f"{MODULE}.BearerTokenCredentialPolicy")
    @mock.patch(f"{MODULE}.DefaultAzureCredential")
    def test_init_with_identity(self, mock_default_azure_credential, mock_policy, mock_request):
        mock_request.return_value.http_request.headers = {"Authorization": "Bearer token"}

        adapter = AzureIdentityCredentialAdapter(
            managed_identity_client_id="managed_identity_client_id",
            workload_identity_tenant_id="workload_identity_tenant_id",
            additionally_allowed_tenants=["workload_identity_tenant_id"],
        )
        mock_default_azure_credential.assert_called_once_with(
            managed_identity_client_id="managed_identity_client_id",
            workload_identity_tenant_id="workload_identity_tenant_id",
            additionally_allowed_tenants=["workload_identity_tenant_id"],
        )
        mock_policy.assert_called_once()

        adapter.signed_session()
        assert adapter.token == {"access_token": "token"}


@pytest.mark.parametrize(
    "host, login, expected_url",
    [
        (None, None, "https://None.blob.core.windows.net/"),  # to maintain existing behaviour
        (None, "storage_account", "https://storage_account.blob.core.windows.net/"),
        ("testaccountname.blob.core.windows.net", None, "https://testaccountname.blob.core.windows.net"),
        (
            "testaccountname.blob.core.windows.net",
            "service_principal_id",
            "https://testaccountname.blob.core.windows.net",
        ),
        (
            "https://testaccountname.blob.core.windows.net",
            None,
            "https://testaccountname.blob.core.windows.net",
        ),
        (
            "https://testaccountname.blob.core.windows.net",
            "service_principal_id",
            "https://testaccountname.blob.core.windows.net",
        ),
    ],
)
def test_parse_blob_account_url(host, login, expected_url):
    assert parse_blob_account_url(host, login) == expected_url
