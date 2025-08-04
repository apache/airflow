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

from airflow.providers.jenkins.hooks.jenkins import JenkinsHook

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"


class TestJenkinsHook:
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_client_created_default_http(self, get_connection_mock):
        """tests `init` method to validate http client creation when all parameters are passed"""
        default_connection_id = "jenkins_default"

        connection_host = "test.com"
        connection_port = 8080
        get_connection_mock.return_value = mock.Mock(
            connection_id=default_connection_id,
            login="test",
            password="test",
            schema="",
            extra_dejson={"use_https": False},
            host=connection_host,
            port=connection_port,
        )

        complete_url = f"http://{connection_host}:{connection_port}/"
        hook = JenkinsHook(default_connection_id)
        assert hook.jenkins_server is not None
        assert hook.jenkins_server.server == complete_url

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_client_created_default_https(self, get_connection_mock):
        """tests `init` method to validate https client creation when all
        parameters are passed"""
        default_connection_id = "jenkins_default"

        connection_host = "test.com"
        connection_port = 8080
        get_connection_mock.return_value = mock.Mock(
            connection_id=default_connection_id,
            login="test",
            password="test",
            schema="",
            extra_dejson={"use_https": True},
            host=connection_host,
            port=connection_port,
        )

        complete_url = f"https://{connection_host}:{connection_port}/"
        hook = JenkinsHook(default_connection_id)
        assert hook.jenkins_server is not None
        assert hook.jenkins_server.server == complete_url

    @pytest.mark.parametrize("param_building", [True, False])
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    @mock.patch("jenkins.Jenkins.get_job_info")
    @mock.patch("jenkins.Jenkins.get_build_info")
    def test_get_build_building_state(
        self, mock_get_build_info, mock_get_job_info, get_connection_mock, param_building
    ):
        get_connection_mock.return_value = mock.Mock(
            connection_id="test_connection",
            login="test",
            password="test",
            schema="",
            extra_dejson={"use_https": False},
            host="test.com",
            port=8080,
        )
        mock_get_build_info.return_value = {"building": param_building}

        hook = JenkinsHook("none_connection_id")
        result = hook.get_build_building_state("some_job", 1)
        assert result == param_building
