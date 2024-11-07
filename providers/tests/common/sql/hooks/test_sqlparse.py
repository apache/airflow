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

from airflow.providers.common.sql.hooks.sql import DbApiHook


@pytest.mark.parametrize(
    "line,parsed_statements",
    [
        ("SELECT * FROM table", ["SELECT * FROM table"]),
        ("SELECT * FROM table;", ["SELECT * FROM table;"]),
        ("SELECT * FROM table; # comment", ["SELECT * FROM table;"]),
        ("SELECT * FROM table; # comment;", ["SELECT * FROM table;"]),
        (" SELECT * FROM table ; # comment;", ["SELECT * FROM table ;"]),
        (
            "SELECT * FROM table; SELECT * FROM table2 # comment",
            ["SELECT * FROM table;", "SELECT * FROM table2"],
        ),
    ],
)
def test_sqlparse(line, parsed_statements):
    assert DbApiHook.split_sql_string(line) == parsed_statements
