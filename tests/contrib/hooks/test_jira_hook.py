# -*- coding: utf-8 -*-
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

from mock import Mock
from mock import patch

from airflow.contrib.hooks.jira_hook import JiraHook
from airflow.models import Connection
from airflow.utils import db

jira_client_mock = Mock(
    name="jira_client"
)


class TestJiraHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='jira_default', conn_type='jira',
                host='https://localhost/jira/', port=443,
                extra='{"verify": "False", "project": "AIRFLOW"}'))

    @patch("airflow.contrib.hooks.jira_hook.JIRA", autospec=True,
           return_value=jira_client_mock)
    def test_jira_client_connection(self, jira_mock):
        jira_hook = JiraHook()

        self.assertTrue(jira_mock.called)
        self.assertIsInstance(jira_hook.client, Mock)
        self.assertEqual(jira_hook.client.name, jira_mock.return_value.name)


if __name__ == '__main__':
    unittest.main()
