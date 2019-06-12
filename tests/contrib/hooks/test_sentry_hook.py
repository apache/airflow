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

import unittest
from unittest.mock import patch, ANY

from airflow.models import Connection
from airflow.contrib.hooks.sentry_hook import SentryHook


class TestSentryHook(unittest.TestCase):
    def setUp(self):
        self.sentry_hook = SentryHook("sentry_default")

    @patch(
        "airflow.contrib.hooks.sentry_hook.SentryHook.get_connection",
        return_value=Connection(host="https://foo@sentry.io/123"),
    )
    @patch("airflow.contrib.hooks.sentry_hook.init")
    def test_get_conn(self, mock_sentry, mock_get_connection):
        """
        Test getting Sentry SDK connection.
        """
        expected_conn = mock_get_connection.return_value

        sentry = self.sentry_hook.get_conn()

        mock_sentry.assert_called_once_with(dsn=expected_conn.host, integrations=[ANY])
        self.assertEqual(sentry, mock_sentry.return_value)
