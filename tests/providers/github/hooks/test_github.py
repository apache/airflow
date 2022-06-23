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
#

import unittest
from unittest.mock import Mock, patch

from airflow.models import Connection
from airflow.providers.github.hooks.github import GithubHook
from airflow.utils import db

github_client_mock = Mock(name="github_client_for_test")


class TestGithubHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='github_default',
                conn_type='github',
                host='https://localhost/github/',
                port=443,
                extra='{"verify": "False", "project": "AIRFLOW"}',
            )
        )

    @patch(
        "airflow.providers.github.hooks.github.GithubClient", autospec=True, return_value=github_client_mock
    )
    def test_github_client_connection(self, github_mock):
        github_hook = GithubHook()

        assert github_mock.called
        assert isinstance(github_hook.client, Mock)
        assert github_hook.client.name == github_mock.return_value.name
