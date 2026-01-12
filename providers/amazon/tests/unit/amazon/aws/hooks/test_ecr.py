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

import boto3
import pytest
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from airflow.providers.amazon.aws.hooks.ecr import EcrHook


@pytest.fixture
def patch_hook(monkeypatch):
    """Patch hook object by dummy boto3 ECR client."""
    ecr_client = boto3.client("ecr")
    monkeypatch.setattr(EcrHook, "conn", ecr_client)


@mock_aws
class TestEcrHook:
    def test_service_type(self):
        """Test expected boto3 client type."""
        assert EcrHook().client_type == "ecr"

    @pytest.mark.parametrize(
        "accounts_ids",
        [
            pytest.param("", id="empty-string"),
            pytest.param(None, id="none"),
            pytest.param([], id="empty-list"),
        ],
    )
    def test_get_temporary_credentials_default_account_id(self, patch_hook, accounts_ids):
        """Test different types of empty account/registry ids."""
        result = EcrHook().get_temporary_credentials(registry_ids=accounts_ids)
        assert len(result) == 1
        assert result[0].username == "AWS"
        assert result[0].registry.startswith(DEFAULT_ACCOUNT_ID)
        assert result[0].password == f"{DEFAULT_ACCOUNT_ID}-auth-token"

    @pytest.mark.parametrize(
        ("accounts_id", "expected_registry"),
        [
            pytest.param(DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_ID, id="moto-default-account"),
            pytest.param("111100002222", "111100002222", id="custom-account-id"),
            pytest.param(["333366669999"], "333366669999", id="custom-account-id-list"),
        ],
    )
    def test_get_temporary_credentials_single_account_id(self, patch_hook, accounts_id, expected_registry):
        """Test different types of single account/registry ids."""
        result = EcrHook().get_temporary_credentials(registry_ids=accounts_id)
        assert len(result) == 1
        assert result[0].username == "AWS"
        assert result[0].registry.startswith(expected_registry)
        assert result[0].password == f"{expected_registry}-auth-token"

    @pytest.mark.parametrize(
        "accounts_ids",
        [
            pytest.param([DEFAULT_ACCOUNT_ID, "111100002222"], id="moto-default-and-custom-account-ids"),
            pytest.param(["999888777666", "333366669999", "777"], id="custom-accounts-ids"),
        ],
    )
    def test_get_temporary_credentials_multiple_account_ids(self, patch_hook, accounts_ids):
        """Test multiple account ids in the single method call."""
        expected_creds = len(accounts_ids)
        result = EcrHook().get_temporary_credentials(registry_ids=accounts_ids)
        assert len(result) == expected_creds
        assert [cr.username for cr in result] == ["AWS"] * expected_creds
        assert all(cr.registry.startswith(accounts_ids[ix]) for ix, cr in enumerate(result))

    @pytest.mark.parametrize(
        "accounts_ids",
        [
            pytest.param(None, id="none"),
            pytest.param("111100002222", id="single-account-id"),
            pytest.param(["999888777666", "333366669999", "777"], id="multiple-account-ids"),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.ecr.mask_secret")
    def test_get_temporary_credentials_mask_secrets(self, mock_masker, patch_hook, accounts_ids):
        """Test masking passwords."""
        result = EcrHook().get_temporary_credentials(registry_ids=accounts_ids)
        assert mock_masker.call_args_list == [mock.call(cr.password) for cr in result]
