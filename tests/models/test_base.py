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

from airflow.models.base import get_id_collation_args

from tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.mark.parametrize(
    ("dsn", "expected", "extra"),
    [
        pytest.param("postgresql://host/the_database", {}, {}, id="postgres"),
        pytest.param(
            "mysql://host/the_database", {"collation": "utf8mb3_bin"}, {}, id="mysql"
        ),
        pytest.param(
            "mysql+pymsql://host/the_database",
            {"collation": "utf8mb3_bin"},
            {},
            id="mysql+pymsql",
        ),
        pytest.param(
            "mysql://host/the_database",
            {"collation": "ascii"},
            {("database", "sql_engine_collation_for_ids"): "ascii"},
            id="mysql with explicit config",
        ),
        pytest.param(
            "postgresql://host/the_database",
            {"collation": "ascii"},
            {("database", "sql_engine_collation_for_ids"): "ascii"},
            id="postgres with explicit config",
        ),
    ],
)
def test_collation(dsn, expected, extra):
    with conf_vars({("database", "sql_alchemy_conn"): dsn, **extra}):
        assert expected == get_id_collation_args()
