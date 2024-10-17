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


class TestGetConnections:
    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_names",
        [
            # Filters
            (
                {},
                12,
                [
                    "test_plugin",
                    "plugin-a",
                    "plugin-b",
                    "plugin-c",
                    "postload",
                    "priority_weight_strategy_plugin",
                    "decreasing_priority_weight_strategy_plugin",
                    "workday_timetable_plugin",
                    "MetadataCollectionPlugin",
                    "hive",
                    "databricks_workflow",
                    "OpenLineageProviderPlugin",
                ],
            ),
            ({"limit": 3, "offset": 2}, 12, ["plugin-b", "plugin-c", "postload"]),
            ({"limit": 1}, 12, ["test_plugin"]),
        ],
    )
    def test_should_respond_200(
        self, test_client, session, query_params, expected_total_entries, expected_names
    ):
        response = test_client.get("/public/plugins/", params=query_params)
        assert response.status_code == 200

        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [plugin["name"] for plugin in body["plugins"]] == expected_names
