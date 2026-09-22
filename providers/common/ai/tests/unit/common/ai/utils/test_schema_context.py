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

from airflow.providers.common.ai.utils.schema_context import format_columns_for_prompt


class TestFormatColumnsForPrompt:
    @pytest.mark.parametrize(
        ("columns", "expected"),
        [
            pytest.param([], "", id="empty"),
            pytest.param([{"name": "id", "type": "int64"}], "id int64", id="single-column"),
            pytest.param(
                [{"name": "id", "type": "int64"}, {"name": "n", "type": "string"}],
                "id int64, n string",
                id="multiple-columns",
            ),
        ],
    )
    def test_joins_columns_with_comma(self, columns, expected):
        assert format_columns_for_prompt(columns) == expected
