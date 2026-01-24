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

import httpx

from airflowctl.api.datamodels.generated import Config
from airflowctl.api.operations import ConfigOperations


class TestConfigSensitiveDataMasking:
    """
    Test that ConfigOperations correctly handles the API's masked sensitive values.

    Note: The Airflow API already masks sensitive config values by setting
    display_sensitive=False in conf.as_dict(). These tests verify that airflowctl
    correctly preserves that masking through the entire data flow.
    """

    def test_config_list_preserves_api_masking(self):
        """
        Verify that when the API returns masked values, ConfigOperations.list()
        preserves them without accidentally exposing or modifying them.

        This tests the data flow: API JSON response -> Pydantic model -> Python objects
        to ensure masked values like "< hidden >" are preserved exactly as the API sends them.
        """
        # Simulate actual JSON response format from /config endpoint
        # This matches the structure from airflow.api_fastapi.core_api.datamodels.config
        api_json_response = {
            "sections": [
                {
                    "name": "core",
                    "options": [
                        {"key": "parallelism", "value": "32"},
                        {"key": "fernet_key", "value": "< hidden >"},
                    ],
                },
                {
                    "name": "database",
                    "options": [
                        {"key": "sql_alchemy_conn", "value": "< hidden >"},
                        {"key": "sql_alchemy_pool_size", "value": "5"},
                    ],
                },
            ]
        }

        class MockHTTPClient:
            def get(self, endpoint):
                return httpx.Response(200, json=api_json_response)

        # Test the actual data flow
        operations = ConfigOperations(client=MockHTTPClient())
        result = operations.list()

        # Verify the response parses correctly
        assert isinstance(result, Config)
        assert len(result.sections) == 2

        # Verify masked values are preserved exactly as API sent them
        core_section = next(s for s in result.sections if s.name == "core")
        fernet_opt = next(o for o in core_section.options if o.key == "fernet_key")
        assert fernet_opt.value == "< hidden >", (
            "Sensitive value must remain masked as API sent it"
        )

        db_section = next(s for s in result.sections if s.name == "database")
        sql_opt = next(o for o in db_section.options if o.key == "sql_alchemy_conn")
        assert sql_opt.value == "< hidden >", (
            "Sensitive value must remain masked as API sent it"
        )

        # Verify non-sensitive values are unchanged
        parallelism_opt = next(o for o in core_section.options if o.key == "parallelism")
        assert parallelism_opt.value == "32"

        pool_size_opt = next(o for o in db_section.options if o.key == "sql_alchemy_pool_size")
        assert pool_size_opt.value == "5"
