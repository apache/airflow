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

from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.cohere.hooks.cohere import (
    CohereHook,
)


class TestCohereHook:
    """
    Test for CohereHook
    """

    def test__get_api_key(self):
        api_key = "test"
        api_url = "http://some_host.com"
        timeout = 150
        max_retries = 5
        with (
            patch.object(
                CohereHook,
                "get_connection",
                return_value=Connection(
                    conn_type="cohere", password=api_key, host=api_url
                ),
            ),
            patch("cohere.Client") as client,
        ):
            hook = CohereHook(timeout=timeout, max_retries=max_retries)
            _ = hook.get_conn
            client.assert_called_once_with(
                api_key=api_key, timeout=timeout, max_retries=max_retries, api_url=api_url
            )
