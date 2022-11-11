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

import unittest
from unittest import mock

from parameterized import parameterized

from airflow.providers.jenkins.hooks.jenkins import JenkinsHook


class TestJenkinsHook(unittest.TestCase):
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_client_created_default_http(self, get_connection_mock):
        """tests `init` method to validate http client creation when all parameters are passed"""
        default_connection_id = "jenkins_default"

        connection_host = "http://test.com"
        connection_port = 8080
        get_connection_mock.return_value = mock.Mock(
            connection_id=default_connection_id,
            login="test",
            password="test",
            extra="",
            host=connection_host,
            port=connection_port,
        )

        complete_url = f"http://{connection_host}:{connection_port}/"
        hook = JenkinsHook(default_connection_id)
        assert hook.jenkins_server is not None
        assert hook.jenkins_server.server == complete_url

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_client_created_default_https(self, get_connection_mock):
        """tests `init` method to validate https client creation when all
        parameters are passed"""
        default_connection_id = "jenkins_default"

        connection_host = "http://test.com"
        connection_port = 8080
        get_connection_mock.return_value = mock.Mock(
            connection_id=default_connection_id,
            login="test",
            password="test",
            extra="true",
            host=connection_host,
            port=connection_port,
        )

        complete_url = f"https://{connection_host}:{connection_port}/"
        hook = JenkinsHook(default_connection_id)
        assert hook.jenkins_server is not None
        assert hook.jenkins_server.server == complete_url

    @parameterized.expand([(True,), (False,)])
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    @mock.patch("jenkins.Jenkins.get_job_info")
    @mock.patch("jenkins.Jenkins.get_build_info")
    def test_get_build_building_state(
        self, param_building, mock_get_build_info, mock_get_job_info, get_connection_mock
    ):
        mock_get_build_info.return_value = {"building": param_building}

        hook = JenkinsHook("none_connection_id")
        result = hook.get_build_building_state("some_job", 1)
        assert result == param_building
