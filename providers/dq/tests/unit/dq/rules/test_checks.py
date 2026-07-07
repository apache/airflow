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

from airflow.providers.dq.rules import CHECK_SPECS


class TestCheckSpecs:
    def test_every_column_check_expression_renders(self):
        for name, spec in CHECK_SPECS.items():
            if spec.requires_column:
                assert "{column}" in spec.expression, name

    def test_row_count_is_the_only_table_level_check(self):
        table_level = {name for name, spec in CHECK_SPECS.items() if not spec.requires_column}
        assert table_level == {"row_count"}
