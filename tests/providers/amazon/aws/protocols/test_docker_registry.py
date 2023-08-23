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

from datetime import datetime, timedelta, timezone
from unittest import mock

import boto3
import pytest
import time_machine
from moto import mock_ecr
from moto.core import DEFAULT_ACCOUNT_ID

from airflow.providers.amazon.aws.protocols.docker_registry import EcrDockerRegistryAuthProtocol, EcrHook


@pytest.fixture
def patch_hook(monkeypatch):
    """Patch hook object by dummy boto3 ECR client."""
    ecr_client = boto3.client("ecr")
    monkeypatch.setattr(EcrHook, "conn", ecr_client)
    yield


@mock_ecr
class TestEcrDockerRegistryAuthProtocol:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, patch_hook):
        with mock.patch.object(EcrHook, "service_config", new_callable=mock.PropertyMock) as m:
            m.return_value = {}
            self.mocked_service_config = m
            yield

    def test_get_refresh_credentials(self):
        auth = EcrDockerRegistryAuthProtocol()
        assert auth.need_refresh is False
        assert auth.expires_at is None

        creds = auth.get_credentials(conn=None)
        assert creds
        assert auth.expires_at is not None
        moto_expired_at = auth.expires_at

        assert creds[0].username == "AWS"
        assert creds[0].registry.startswith(DEFAULT_ACCOUNT_ID)
        assert creds[0].password == f"{DEFAULT_ACCOUNT_ID}-auth-token"
        assert creds[0].reauth is True

        with time_machine.travel(moto_expired_at - timedelta(hours=12), tick=False):
            assert auth.need_refresh is False

        with time_machine.travel(moto_expired_at, tick=False):
            assert auth.need_refresh is True

        with time_machine.travel(moto_expired_at - timedelta(minutes=4), tick=False):
            assert auth.need_refresh is True

        with time_machine.travel(moto_expired_at - timedelta(minutes=5), tick=False):
            assert auth.need_refresh is False

        auth.expires_at = datetime(3000, 1, 1, tzinfo=timezone.utc)
        auth.refresh_credentials(conn=None)
        assert auth.expires_at == moto_expired_at

    @pytest.mark.parametrize(
        "auth_registry_ids, conn_registry_ids, expected",
        [
            pytest.param(None, None, None, id="both-not-set"),
            pytest.param(
                ["111100002222", "333366669999"], None, ["111100002222", "333366669999"], id="auth-arg-set"
            ),
            pytest.param(None, "999888777666", "999888777666", id="conn-set"),
            pytest.param("333366669999", "999888777666", "333366669999", id="both-set"),
        ],
    )
    def test_resolve_registry_ids(self, auth_registry_ids, conn_registry_ids, expected):
        auth = EcrDockerRegistryAuthProtocol(registry_ids=auth_registry_ids)
        self.mocked_service_config.return_value = {"registry_ids": conn_registry_ids}
        with mock.patch.object(EcrHook, "get_temporary_credentials") as mocked_get_temp_creds:
            mocked_get_temp_creds.return_value = []
            auth.get_credentials(conn=None)
            mocked_get_temp_creds.assert_called_once_with(registry_ids=expected)
