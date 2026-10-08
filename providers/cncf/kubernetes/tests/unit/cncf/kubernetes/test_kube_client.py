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

from airflow.providers.cncf.kubernetes.kube_client import (
    _get_executor_async_kube_client,
    _get_executor_kube_client,
    _TimeoutAsyncK8sApiClient,
    get_async_kube_client,
)
from airflow.providers.common.compat.sdk import AirflowConfigException

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

FACTORY_MARKER = "client-from-factory"
OTHER_MARKER = "client-from-other-factory"


def build_fake_client():
    return FACTORY_MARKER


def build_other_client():
    return OTHER_MARKER


class TestGetExecutorKubeClient:
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client", autospec=True)
    def test_without_factory_uses_default_client(self, mock_get_kube_client):
        client = _get_executor_kube_client()

        assert client is mock_get_kube_client.return_value
        mock_get_kube_client.assert_called_once_with()

    @conf_vars({("kubernetes_executor", "client_factory"): f"{__name__}.build_fake_client"})
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client", autospec=True)
    def test_factory_replaces_default_client(self, mock_get_kube_client):
        client = _get_executor_kube_client()

        assert client == FACTORY_MARKER
        mock_get_kube_client.assert_not_called()

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team config requires Airflow 3.2+")
    @conf_vars({("kubernetes_executor", "client_factory"): f"{__name__}.build_other_client"})
    def test_team_factory_takes_precedence_over_the_global_one(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__CLIENT_FACTORY", f"{__name__}.build_fake_client"
        )

        assert _get_executor_kube_client(team_name="team_a") == FACTORY_MARKER

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team config requires Airflow 3.2+")
    @conf_vars({("kubernetes_executor", "client_factory"): f"{__name__}.build_fake_client"})
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client", autospec=True)
    def test_team_without_factory_does_not_use_the_global_one(self, mock_get_kube_client, monkeypatch):
        """Team config does not inherit the un-prefixed section, so team_b gets neither factory."""
        monkeypatch.setenv(
            "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__CLIENT_FACTORY", f"{__name__}.build_other_client"
        )

        client = _get_executor_kube_client(team_name="team_b")

        assert client is mock_get_kube_client.return_value
        mock_get_kube_client.assert_called_once_with()

    @conf_vars({("kubernetes_executor", "client_factory"): "no.such.module.build_client"})
    def test_unimportable_factory_raises(self):
        with pytest.raises(AirflowConfigException):
            _get_executor_kube_client()


class TestGetExecutorAsyncKubeClient:
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_async_kube_client", autospec=True)
    async def test_without_factory_uses_default_client(self, mock_get_async_kube_client):
        client = await _get_executor_async_kube_client()

        assert client is mock_get_async_kube_client.return_value
        mock_get_async_kube_client.assert_awaited_once_with()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_async_kube_client", autospec=True)
    async def test_async_factory_replaces_default_client(self, mock_get_async_kube_client):
        with conf_vars({("kubernetes_executor", "async_client_factory"): f"{__name__}.build_fake_client"}):
            client = await _get_executor_async_kube_client()

        assert client == FACTORY_MARKER
        mock_get_async_kube_client.assert_not_called()

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team config requires Airflow 3.2+")
    @pytest.mark.asyncio
    async def test_team_async_factory_takes_precedence_over_the_global_one(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__ASYNC_CLIENT_FACTORY",
            f"{__name__}.build_fake_client",
        )
        with conf_vars({("kubernetes_executor", "async_client_factory"): f"{__name__}.build_other_client"}):
            client = await _get_executor_async_kube_client(team_name="team_a")

        assert client == FACTORY_MARKER


class TestGetAsyncKubeClient:
    @pytest.mark.asyncio
    @mock.patch("kubernetes_asyncio.config.load_incluster_config")
    async def test_wraps_client_with_request_timeout(self, mock_load_incluster):
        """The async client carries the shared client-side request-timeout wrapper."""
        with conf_vars(
            {("kubernetes_executor", "verify_ssl"): "True", ("kubernetes_executor", "ssl_ca_cert"): ""}
        ):
            api = await get_async_kube_client(in_cluster=True)

        assert isinstance(api.api_client, _TimeoutAsyncK8sApiClient)
        mock_load_incluster.assert_called_once()
