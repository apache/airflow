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

import warnings
from collections.abc import Generator
from datetime import timedelta

import pytest
from sqlalchemy.exc import SAWarning

from airflow.dag_processing.collection import AssetModelOperation, _get_latest_runs_stmt
from airflow.models import DagModel, Trigger
from airflow.models.asset import (
    AssetActive,
    asset_trigger_association_table,
)
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk.definitions.asset import Asset
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_assets, clear_db_dags, clear_db_triggers


def test_statement_latest_runs_one_dag():
    with warnings.catch_warnings():
        warnings.simplefilter("error", category=SAWarning)

        stmt = _get_latest_runs_stmt(["fake-dag"])
        compiled_stmt = str(stmt.compile())
        actual = [x.strip() for x in compiled_stmt.splitlines()]
        expected = [
            "SELECT dag_run.id, dag_run.dag_id, dag_run.logical_date, "
            "dag_run.data_interval_start, dag_run.data_interval_end",
            "FROM dag_run",
            "WHERE dag_run.dag_id = :dag_id_1 AND dag_run.logical_date = ("
            "SELECT max(dag_run.logical_date) AS max_logical_date",
            "FROM dag_run",
            "WHERE dag_run.dag_id = :dag_id_2 AND dag_run.run_type IN (__[POSTCOMPILE_run_type_1]))",
        ]
        assert actual == expected, compiled_stmt


def test_statement_latest_runs_many_dag():
    with warnings.catch_warnings():
        warnings.simplefilter("error", category=SAWarning)

        stmt = _get_latest_runs_stmt(["fake-dag-1", "fake-dag-2"])
        compiled_stmt = str(stmt.compile())
        actual = [x.strip() for x in compiled_stmt.splitlines()]
        expected = [
            "SELECT dag_run.id, dag_run.dag_id, dag_run.logical_date, "
            "dag_run.data_interval_start, dag_run.data_interval_end",
            "FROM dag_run, (SELECT dag_run.dag_id AS dag_id, "
            "max(dag_run.logical_date) AS max_logical_date",
            "FROM dag_run",
            "WHERE dag_run.dag_id IN (__[POSTCOMPILE_dag_id_1]) "
            "AND dag_run.run_type IN (__[POSTCOMPILE_run_type_1]) GROUP BY dag_run.dag_id) AS anon_1",
            "WHERE dag_run.dag_id = anon_1.dag_id AND dag_run.logical_date = anon_1.max_logical_date",
        ]
        assert actual == expected, compiled_stmt


@pytest.mark.db_test
class TestAssetModelOperation:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_assets()
        clear_db_triggers()

    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        self.clean_db()
        yield
        self.clean_db()

    @pytest.mark.parametrize(
        "is_active, is_paused, expected_num_triggers",
        [
            (True, True, 0),
            (True, False, 1),
            (False, True, 0),
            (False, False, 0),
        ],
    )
    def test_add_asset_trigger_references(self, is_active, is_paused, expected_num_triggers, dag_maker):
        trigger = TimeDeltaTrigger(timedelta(seconds=0))
        asset = Asset("test_add_asset_trigger_references_asset", watchers=[trigger])

        with dag_maker(dag_id="test_add_asset_trigger_references_dag", schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

            asset_op = AssetModelOperation.collect({"test_add_asset_trigger_references_dag": dag})

        with create_session() as session:
            # Update `is_active` and `is_paused` properties from DAG
            dags = session.query(DagModel).all()
            for dag in dags:
                dag.is_active = is_active
                dag.is_paused = is_paused

            orm_assets = asset_op.add_assets(session=session)
            # Create AssetActive objects from assets. It is usually done in the scheduler
            for asset in orm_assets.values():
                session.add(AssetActive.for_asset(asset))
            session.commit()

            asset_op.add_asset_trigger_references(orm_assets, session=session)

            session.commit()

            assert session.query(Trigger).count() == expected_num_triggers
            assert session.query(asset_trigger_association_table).count() == expected_num_triggers
