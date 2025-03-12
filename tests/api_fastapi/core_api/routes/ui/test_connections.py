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

import pytest

pytestmark = pytest.mark.db_test


class TestHookMetaData:
    def test_hook_meta_data(self, test_client):
        response = test_client.get("/ui/connections/hook_meta")
        response_data = response.json()
        assert any(hook_data["connection_type"] == "generic" for hook_data in response_data)
        assert any(hook_data["connection_type"] == "fs" for hook_data in response_data)

        for hook_data in response_data:
            if hook_data["connection_type"] == "fs":
                assert hook_data["hook_name"] == "File (path)"

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/ui/connections/hook_meta")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/ui/connections/hook_meta")
        assert response.status_code == 403
