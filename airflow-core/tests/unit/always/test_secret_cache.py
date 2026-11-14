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

import os
import subprocess
import sys
import textwrap
from unittest import mock

import pytest

from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.sdk import SecretCache as SdkSecretCache
from airflow.sdk.execution_time import task_runner
from airflow.secrets.cache import SecretCache

from tests_common.test_utils.config import conf_vars


@pytest.fixture(autouse=True)
def reset_cache():
    SecretCache.reset()
    SdkSecretCache.reset()
    yield
    SecretCache.reset()
    SdkSecretCache.reset()


@conf_vars({("secrets", "use_cache"): "true", ("secrets", "cache_ttl_seconds"): "30"})
def test_parsing_loop_shares_cache_with_in_process_api():
    def stop_polling():
        raise RuntimeError("stop before polling")

    manager = DagFileProcessorManager(max_runs=0, heartbeat=stop_polling)
    with pytest.raises(RuntimeError, match="stop before polling"):
        manager._run_parsing_loop()

    SdkSecretCache.save_variable("key", "value", team_name="team")
    assert SecretCache.get_variable("key", team_name="team") == "value"
    SecretCache.invalidate_variable("key", team_name="team")
    with pytest.raises(SdkSecretCache.NotPresentException):
        SdkSecretCache.get_variable("key", team_name="team")

    SecretCache.save_connection_uri("connection", "http://example.com")
    assert SdkSecretCache.get_connection_uri("connection") == "http://example.com"


@mock.patch.object(SdkSecretCache, "init", autospec=True)
@mock.patch.object(SdkSecretCache, "_cache", {})
def test_parsing_loop_does_not_share_cache_with_http_api(mock_init):
    def stop_polling():
        raise RuntimeError("stop before polling")

    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    manager = DagFileProcessorManager(max_runs=0, heartbeat=stop_polling, api_client=client)
    with pytest.raises(RuntimeError, match="stop before polling"):
        manager._run_parsing_loop()

    mock_init.assert_not_called()
    assert SecretCache._cache is None


@mock.patch.object(SdkSecretCache, "_cache", {})
@mock.patch.object(SecretCache, "_cache", {})
def test_http_api_context_detaches_both_secret_caches(monkeypatch):
    monkeypatch.setenv("_AIRFLOW_PROCESS_CONTEXT", "server")
    monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", None, raising=False)
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    manager = DagFileProcessorManager(max_runs=0, api_client=client)

    manager.prepare_api_secrets_context(client)

    assert SdkSecretCache._cache is None
    assert SecretCache._cache is None


def test_core_cache_can_run_without_task_sdk():
    code = textwrap.dedent(
        """
        import sys

        class NoSdkImports:
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "airflow.sdk" or fullname.startswith("airflow.sdk."):
                    raise ModuleNotFoundError(fullname, name=fullname)

        sys.meta_path.insert(0, NoSdkImports())

        from airflow.secrets.cache import SecretCache

        SecretCache._initialize(ttl_seconds=30)
        SecretCache.save_variable("key", "value", team_name="team")
        assert SecretCache.get_variable("key", team_name="team") == "value"
        SecretCache.invalidate_variable("key", team_name="team")
        try:
            SecretCache.get_variable("key", team_name="team")
        except SecretCache.NotPresentException:
            pass
        else:
            raise AssertionError("cache entry was not invalidated")
        """
    )
    subprocess.run(
        [sys.executable, "-c", code],
        env={
            **os.environ,
            "_AIRFLOW__AS_LIBRARY": "true",
        },
        check=True,
        timeout=30,
    )
