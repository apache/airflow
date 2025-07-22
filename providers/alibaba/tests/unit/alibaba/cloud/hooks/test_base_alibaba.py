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

from airflow.providers.alibaba.cloud.hooks.base_alibaba import AccessKeyCredentials, AlibabaBaseHook

BASE_ALIBABA_HOOK_MODULE = "airflow.providers.alibaba.cloud.hooks.base_alibaba.{}"
MOCK_MAXCOMPUTE_CONN_ID = "mock_id"
MOCK_ACCESS_KEY_ID = "mock_access_key_id"
MOCK_ACCESS_KEY_SECRET = "mock_access_key_secret"


class TestAlibabaBaseHook:
    def setup_method(self):
        with mock.patch(
            BASE_ALIBABA_HOOK_MODULE.format("AlibabaBaseHook.get_connection"),
        ) as mock_get_connection:
            mock_conn = mock.MagicMock()
            mock_conn.extra_dejson = {
                "access_key_id": MOCK_ACCESS_KEY_ID,
                "access_key_secret": MOCK_ACCESS_KEY_SECRET,
            }

            mock_get_connection.return_value = mock_conn
            self.hook = AlibabaBaseHook(alibabacloud_conn_id=MOCK_MAXCOMPUTE_CONN_ID)

    def test_get_access_key_credential(self):
        creds = AccessKeyCredentials(
            access_key_id=MOCK_ACCESS_KEY_ID,
            access_key_secret=MOCK_ACCESS_KEY_SECRET,
        )

        creds = self.hook.get_access_key_credential()

        assert creds.access_key_id == MOCK_ACCESS_KEY_ID
        assert creds.access_key_secret == MOCK_ACCESS_KEY_SECRET
