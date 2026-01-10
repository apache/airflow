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

import json

import httpx
import pytest

from airflowctl.api.datamodels.generated import Config
from airflowctl.api.operations import ConfigOperations


class TestConfigOperationsSensitiveData:
    """Test that ConfigOperations correctly handles masked sensitive data from the API."""

    @pytest.fixture
    def mock_client(self, monkeypatch):
        """Mock client that returns config with masked sensitive values."""

        class MockClient:
            def get(self, endpoint):
                # Simulate API response with masked sensitive values
                mock_response_data = {
                    "sections": [
                        {
                            "name": "core",
                            "options": [
                                {"key": "parallelism", "value": "32"},
                                {"key": "fernet_key", "value": "< hidden >"},  # Sensitive!
                            ],
                        },
                        {
                            "name": "database",
                            "options": [
                                {"key": "sql_alchemy_conn", "value": "< hidden >"},  # Sensitive!
                            ],
                        },
                    ]
                }
                return httpx.Response(200, json=mock_response_data)

        return MockClient()

    def test_list_config_masks_sensitive_values(self, mock_client):
        """Test that ConfigOperations.list() correctly receives masked sensitive values."""
        operations = ConfigOperations(client=mock_client)
        result = operations.list()

        assert isinstance(result, Config)

        # Find core section
        core_section = next((s for s in result.sections if s.name == "core"), None)
        assert core_section is not None

        # Verify fernet_key is masked
        fernet_key_option = next((o for o in core_section.options if o.key == "fernet_key"), None)
        assert fernet_key_option is not None
        assert fernet_key_option.value == "< hidden >", "fernet_key should be masked"

        # Find database section
        db_section = next((s for s in result.sections if s.name == "database"), None)
        assert db_section is not None

        # Verify sql_alchemy_conn is masked
        sql_conn_option = next((o for o in db_section.options if o.key == "sql_alchemy_conn"), None)
        assert sql_conn_option is not None
        assert sql_conn_option.value == "< hidden >", "sql_alchemy_conn should be masked"

        # Verify non-sensitive values are NOT masked
        parallelism_option = next((o for o in core_section.options if o.key == "parallelism"), None)
        assert parallelism_option is not None
        assert parallelism_option.value == "32", "Non-sensitive values should not be masked"
