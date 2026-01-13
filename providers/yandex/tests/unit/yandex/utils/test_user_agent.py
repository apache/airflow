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

from airflow.providers.yandex.utils.user_agent import provider_user_agent

yandexcloud = pytest.importorskip("yandexcloud")


def test_provider_user_agent():
    user_agent = provider_user_agent()

    from airflow import __version__ as airflow_version

    user_agent_airflow = f"apache-airflow/{airflow_version}"
    assert user_agent_airflow in user_agent

    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    provider_name = manager.hooks["yandexcloud"].package_name
    provider = manager.providers[provider_name]
    user_agent_provider = f"{provider_name}/{provider.version}"
    assert user_agent_provider in user_agent

    from airflow.providers.common.compat.sdk import conf

    user_agent_prefix = conf.get("yandex", "sdk_user_agent_prefix", fallback="")
    assert user_agent_prefix in user_agent


@mock.patch("airflow.providers_manager.ProvidersManager.hooks")
def test_provider_user_agent_hook_not_exists(mock_hooks):
    mock_hooks.return_value = []

    user_agent = provider_user_agent()

    assert user_agent is None
