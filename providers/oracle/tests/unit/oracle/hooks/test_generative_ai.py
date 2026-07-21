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

from oci.generative_ai import GenerativeAiClient

from airflow.providers.oracle.hooks.generative_ai import OciGenerativeAIHook


class TestOciGenerativeAIHook:
    @mock.patch.object(OciGenerativeAIHook, "get_client", autospec=True)
    def test_get_conn_creates_and_caches_native_client(self, mock_get_client):
        hook = OciGenerativeAIHook()
        client = mock_get_client.return_value

        assert hook.get_conn() is client
        assert hook.conn is client
        mock_get_client.assert_called_once_with(hook, GenerativeAiClient)
