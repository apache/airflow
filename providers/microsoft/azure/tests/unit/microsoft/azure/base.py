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

from contextlib import contextmanager
from unittest.mock import patch

from kiota_http.httpx_request_adapter import HttpxRequestAdapter

from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook

from unit.microsoft.azure.test_utils import get_airflow_connection


class Base:
    def teardown_method(self, method):
        KiotaRequestAdapterHook.cached_request_adapters.clear()

    @contextmanager
    def patch_hook_and_request_adapter(self, response):
        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message") as mock_get_http_response,
        ):
            if isinstance(response, Exception):
                mock_get_http_response.side_effect = response
            else:
                mock_get_http_response.return_value = response
            yield
