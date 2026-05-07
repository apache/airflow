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

from datetime import datetime

import pendulum
import pytest
from sqlalchemy import select

from airflow.cli import cli_parser
from airflow.cli.commands import partition_command
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import CronPartitionTimetable
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


DAG_ID = "test_partitions_clear_dag"


@pytest.fixture
def parser():
    return cli_parser.get_parser()


@pytest.fixture
def setup_partitioned_runs(dag_maker):
    clear_db_runs()
    clear_db_dags()
    with dag_maker(
        DAG_ID,
        schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
        start_date=datetime(2026, 1, 1),
        catchup=True,
        serialized=True,
    ):
        EmptyOperator(task_id="t1")
    dag_maker.create_dagrun(
        run_id="part_run_1",
        state=DagRunState.SUCCESS,
        logical_date=None,
        partition_date=datetime(2026, 1, 1, tzinfo=pendulum.UTC),
        partition_key="2026-01-01T00:00:00",
    )
    dag_maker.create_dagrun(
        run_id="part_run_2",
        state=DagRunState.SUCCESS,
        logical_date=None,
        partition_date=datetime(2026, 1, 2, tzinfo=pendulum.UTC),
        partition_key="2026-01-02T00:00:00",
    )
    dag_maker.create_dagrun(
        run_id="part_run_3",
        state=DagRunState.SUCCESS,
        logical_date=None,
        partition_date=datetime(2026, 1, 3, tzinfo=pendulum.UTC),
        partition_key="2026-01-03T00:00:00",
    )
    dag_maker.sync_dagbag_to_db()
    yield
    clear_db_runs()
    clear_db_dags()


def _get_run(run_id: str) -> DagRun:
    with create_session() as session:
        run = session.scalar(select(DagRun).where(DagRun.run_id == run_id))
    if run is None:
        raise AssertionError(f"DagRun {run_id} not found")
    return run


@pytest.fixture(autouse=True)
def _disable_migration_check():
    with conf_vars({("database", "check_migrations"): "False"}):
        yield


class TestPartitionsClear:
    def test_clear_single_run_id(self, parser, setup_partitioned_runs, capsys):
        partition_command.clear(
            parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--run-id", "part_run_2"])
        )
        captured = capsys.readouterr()
        assert "part_run_2" in captured.out
        assert "Cleared partition fields on 1 DagRun(s)" in captured.out

        run_2 = _get_run("part_run_2")
        assert run_2.partition_key is None
        assert run_2.partition_date is None
        # Other runs unchanged
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        assert run_1.partition_date is not None

    def test_clear_with_date_range(self, parser, setup_partitioned_runs, capsys):
        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    DAG_ID,
                    "--start-date",
                    "2026-01-02",
                    "--end-date",
                    "2026-01-03",
                ]
            )
        )
        captured = capsys.readouterr()
        assert "Cleared partition fields on 2 DagRun(s)" in captured.out

        # Out-of-range run untouched
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        # In-range runs cleared
        for run_id in ("part_run_2", "part_run_3"):
            run = _get_run(run_id)
            assert run.partition_key is None
            assert run.partition_date is None

    def test_dry_run_does_not_modify(self, parser, setup_partitioned_runs, capsys):
        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    DAG_ID,
                    "--start-date",
                    "2026-01-01",
                    "--dry-run",
                ]
            )
        )
        captured = capsys.readouterr()
        assert "Dry run: would clear 3 DagRun(s)" in captured.out

        for run_id in ("part_run_1", "part_run_2", "part_run_3"):
            run = _get_run(run_id)
            assert run.partition_key is not None
            assert run.partition_date is not None

    def test_no_match_prints_message(self, parser, setup_partitioned_runs, capsys):
        partition_command.clear(
            parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--run-id", "does_not_exist"])
        )
        captured = capsys.readouterr()
        assert "No matching DagRuns found" in captured.out

    def test_already_cleared_run_is_skipped(self, parser, setup_partitioned_runs, capsys):
        with create_session() as session:
            run = session.scalar(select(DagRun).where(DagRun.run_id == "part_run_1"))
            run.partition_key = None
            run.partition_date = None
            session.commit()

        partition_command.clear(
            parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--run-id", "part_run_1"])
        )
        captured = capsys.readouterr()
        assert "already cleared" in captured.out
        assert "Cleared partition fields on 0 DagRun(s)" in captured.out

    def test_requires_run_id_or_range(self, parser, setup_partitioned_runs):
        with pytest.raises(SystemExit, match="Specify either --run-id"):
            partition_command.clear(parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID]))

    def test_run_id_and_range_mutually_exclusive(self, parser, setup_partitioned_runs):
        with pytest.raises(SystemExit, match="cannot be combined"):
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        DAG_ID,
                        "--run-id",
                        "part_run_1",
                        "--start-date",
                        "2026-01-01",
                    ]
                )
            )

    def test_prints_before_values(self, parser, setup_partitioned_runs, capsys):
        partition_command.clear(
            parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--run-id", "part_run_1"])
        )
        captured = capsys.readouterr()
        assert "partition_key='2026-01-01T00:00:00' -> None" in captured.out
        assert "partition_date=2026-01-01T00:00:00+00:00 -> None" in captured.out
