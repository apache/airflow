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

from airflow.providers.microsoft.azure.serialization.response_handler import CallableResponseHandler
from tests.providers.microsoft.azure.base import Base
from tests.providers.microsoft.conftest import load_json, mock_json_response


class TestResponseHandler(Base):
    def test_handle_response_async(self):
        users = load_json("resources", "users.json")
        response = mock_json_response(200, users)

        actual = self.run_async(
            CallableResponseHandler(
                lambda response, error_map: response.json()
            ).handle_response_async(response, None)
        )

        assert isinstance(actual, dict)
        assert actual == users
