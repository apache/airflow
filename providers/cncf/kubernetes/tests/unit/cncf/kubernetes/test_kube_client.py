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

import os
import subprocess
import sys
import textwrap
from unittest import mock

import pytest

from airflow.providers.cncf.kubernetes.kube_client import (
    _TimeoutAsyncK8sApiClient,
    get_async_kube_client,
    get_kube_client,
)
from airflow.providers.common.compat.sdk import AirflowConfigException

from tests_common.test_utils.config import conf_vars

FACTORY_MARKER = "client-from-factory"


def build_fake_client():
    return FACTORY_MARKER


class TestKubeClientFactory:
    @pytest.mark.parametrize(
        ("kwargs", "loader"),
        [
            ({"in_cluster": True}, "load_incluster_config"),
            ({"in_cluster": False, "config_file": "/does/not/exist"}, "load_kube_config"),
        ],
    )
    def test_default_client_is_built_when_factory_unset(self, kwargs, loader):
        with (
            conf_vars({("kubernetes_executor", "enable_tcp_keepalive"): "False"}),
            mock.patch(f"kubernetes.config.{loader}") as mock_loader,
        ):
            get_kube_client(**kwargs)

        mock_loader.assert_called_once()

    @mock.patch("kubernetes.config.load_kube_config")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_factory_replaces_default_client_construction(self, mock_incluster, mock_kube_config):
        with conf_vars({("kubernetes_executor", "client_factory"): f"{__name__}.build_fake_client"}):
            client = get_kube_client()

        assert client == FACTORY_MARKER
        mock_incluster.assert_not_called()
        mock_kube_config.assert_not_called()

    def test_unimportable_factory_raises(self):
        with conf_vars({("kubernetes_executor", "client_factory"): "no.such.module.build_client"}):
            with pytest.raises(AirflowConfigException):
                get_kube_client()

    def test_factory_is_resolved_from_config_in_a_separate_interpreter(self, tmp_path, monkeypatch):
        """
        KubernetesJobWatcher builds its client in its own process, which under the spawn start
        method shares no objects with the scheduler. Resolving the import path there has to work
        from configuration alone.
        """
        (tmp_path / "kube_client_factory_fixture.py").write_text(
            textwrap.dedent(
                f"""
                def build_fake_client():
                    return "{FACTORY_MARKER}"
                """
            )
        )
        monkeypatch.setenv(
            "PYTHONPATH", os.pathsep.join(filter(None, [str(tmp_path), os.environ.get("PYTHONPATH", "")]))
        )
        monkeypatch.setenv(
            "AIRFLOW__KUBERNETES_EXECUTOR__CLIENT_FACTORY",
            "kube_client_factory_fixture.build_fake_client",
        )

        child = subprocess.run(
            [
                sys.executable,
                "-c",
                "from airflow.providers.cncf.kubernetes.kube_client import get_kube_client;"
                "print(get_kube_client())",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )

        assert child.returncode == 0, child.stderr
        assert FACTORY_MARKER in child.stdout


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
