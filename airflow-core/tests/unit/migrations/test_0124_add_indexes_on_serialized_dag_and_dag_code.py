#
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
import sqlalchemy as sa

from airflow import settings
from airflow.utils.db import downgrade, upgradedb

pytestmark = pytest.mark.db_test

_REVISION = "3c525f44bea8"
_DOWN_REVISION = "d2f4e1b3c5a7"

_EXPECTED_INDEXES = {
    "idx_serialized_dag_dag_id": ("serialized_dag", "dag_id"),
    "idx_dag_code_dag_id": ("dag_code", "dag_id"),
}


class TestMigration0124AddDagIdIndexes:
    @pytest.fixture(autouse=True)
    def _restore_head(self):
        yield
        upgradedb()

    @staticmethod
    def _get_indexes(table):
        with settings.engine.connect() as conn:
            return {ix["name"]: ix["column_names"] for ix in sa.inspect(conn).get_indexes(table)}

    def test_upgrade_creates_indexes_and_downgrade_drops_them(self):
        downgrade(to_revision=_DOWN_REVISION)
        for index, (table, _) in _EXPECTED_INDEXES.items():
            assert index not in self._get_indexes(table)

        upgradedb(to_revision=_REVISION)
        for index, (table, column) in _EXPECTED_INDEXES.items():
            assert self._get_indexes(table).get(index) == [column]
