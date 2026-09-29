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

_REVISION = "9f8d3473abf9"
_DOWN_REVISION = "90e4d18ccadf"


class TestMigration0142RemoveDuplicateDagRunStateDagIdIndexes:
    @pytest.fixture(autouse=True)
    def _restore_head(self):
        yield
        upgradedb()

    @staticmethod
    def _get_dag_run_indexes():
        with settings.get_engine().connect() as conn:
            return {ix["name"]: ix["column_names"] for ix in sa.inspect(conn).get_indexes("dag_run")}

    @staticmethod
    def _get_dag_run_index_predicates():
        with settings.get_engine().connect() as conn:
            return {
                ix["name"]: next(
                    (str(v) for k, v in ix.get("dialect_options", {}).items() if k.endswith("_where")), None
                )
                for ix in sa.inspect(conn).get_indexes("dag_run")
            }

    @staticmethod
    def _recreate_postgres_indexes(*, partial: bool):
        with settings.get_engine().begin() as conn:
            for index, state in (
                ("idx_dag_run_queued_dags", "queued"),
                ("idx_dag_run_running_dags", "running"),
            ):
                where = f" WHERE state = '{state}'" if partial else ""
                conn.execute(sa.text(f"DROP INDEX {index}"))
                conn.execute(sa.text(f"CREATE INDEX {index} ON dag_run (state, dag_id){where}"))

    @staticmethod
    def _get_postgres_index_oids():
        with settings.get_engine().connect() as conn:
            return {
                index: conn.execute(sa.text(f"SELECT '{index}'::regclass::oid")).scalar()
                for index in ("idx_dag_run_queued_dags", "idx_dag_run_running_dags")
            }

    @pytest.mark.backend("mysql")
    def test_upgrade_drops_index_and_downgrade_recreates_it(self):
        downgrade(to_revision=_DOWN_REVISION)
        assert self._get_dag_run_indexes()["idx_dag_run_queued_dags"] == ["state", "dag_id"]

        upgradedb(to_revision=_REVISION)
        indexes = self._get_dag_run_indexes()
        assert "idx_dag_run_queued_dags" not in indexes
        assert indexes["idx_dag_run_running_dags"] == ["state", "dag_id"]

        downgrade(to_revision=_DOWN_REVISION)
        assert self._get_dag_run_indexes()["idx_dag_run_queued_dags"] == ["state", "dag_id"]

    @pytest.mark.backend("mysql")
    def test_upgrade_succeeds_when_index_was_already_dropped(self):
        downgrade(to_revision=_DOWN_REVISION)
        with settings.get_engine().begin() as conn:
            conn.execute(sa.text("DROP INDEX idx_dag_run_queued_dags ON dag_run"))

        upgradedb(to_revision=_REVISION)
        assert "idx_dag_run_queued_dags" not in self._get_dag_run_indexes()

    @pytest.mark.backend("mysql")
    def test_downgrade_succeeds_when_index_already_exists(self):
        upgradedb(to_revision=_REVISION)
        with settings.get_engine().begin() as conn:
            conn.execute(sa.text("CREATE INDEX idx_dag_run_queued_dags ON dag_run (state, dag_id)"))

        downgrade(to_revision=_DOWN_REVISION)
        assert self._get_dag_run_indexes()["idx_dag_run_queued_dags"] == ["state", "dag_id"]

    @pytest.mark.backend("postgres")
    def test_upgrade_recreates_plain_postgres_indexes_as_partial(self):
        downgrade(to_revision=_DOWN_REVISION)
        self._recreate_postgres_indexes(partial=False)

        upgradedb(to_revision=_REVISION)
        predicates = self._get_dag_run_index_predicates()
        assert "'queued'" in predicates["idx_dag_run_queued_dags"]
        assert "'running'" in predicates["idx_dag_run_running_dags"]

    @pytest.mark.backend("postgres")
    def test_upgrade_leaves_partial_postgres_indexes_untouched(self):
        downgrade(to_revision=_DOWN_REVISION)
        self._recreate_postgres_indexes(partial=True)
        oids_before = self._get_postgres_index_oids()

        upgradedb(to_revision=_REVISION)
        assert self._get_postgres_index_oids() == oids_before

    @pytest.mark.backend("sqlite")
    def test_upgrade_keeps_partial_sqlite_indexes(self):
        downgrade(to_revision=_DOWN_REVISION)
        predicates_before = self._get_dag_run_index_predicates()

        upgradedb(to_revision=_REVISION)
        predicates = self._get_dag_run_index_predicates()
        assert predicates["idx_dag_run_queued_dags"] == predicates_before["idx_dag_run_queued_dags"]
        assert predicates["idx_dag_run_running_dags"] == predicates_before["idx_dag_run_running_dags"]
        assert "'queued'" in predicates["idx_dag_run_queued_dags"]
