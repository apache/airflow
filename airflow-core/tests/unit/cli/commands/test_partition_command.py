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
from unittest import mock

import pendulum
import pytest
from sqlalchemy import select

from airflow.cli import cli_parser
from airflow.cli.commands import partition_command
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import CronPartitionTimetable
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState

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


def _set_tis_state(run_id: str, state: TaskInstanceState) -> None:
    with create_session() as session:
        tis = session.scalars(select(TaskInstance).where(TaskInstance.run_id == run_id)).all()
        for ti in tis:
            ti.state = state
        session.commit()


def _get_tis(run_id: str) -> list[TaskInstance]:
    with create_session() as session:
        return list(session.scalars(select(TaskInstance).where(TaskInstance.run_id == run_id)))


@pytest.mark.usefixtures("setup_partitioned_runs")
class TestPartitionsClear:
    def test_clear_single_run_id(self, parser, capsys):
        partition_command.clear(
            parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--run-id", "part_run_2"])
        )
        captured = capsys.readouterr()
        assert captured.out == (
            "DagRun part_run_2: partition_key='2026-01-02T00:00:00' -> None, "
            "partition_date=2026-01-02T00:00:00+00:00 -> None\n"
            "Cleared partition fields on 1 DagRun(s).\n"
        )

        run_2 = _get_run("part_run_2")
        assert run_2.partition_key is None
        assert run_2.partition_date is None
        # Other runs unchanged
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        assert run_1.partition_date == datetime(2026, 1, 1, tzinfo=pendulum.UTC)

    def test_clear_with_date_range(self, parser, capsys):
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
        assert captured.out == (
            "DagRun part_run_2: partition_key='2026-01-02T00:00:00' -> None, "
            "partition_date=2026-01-02T00:00:00+00:00 -> None\n"
            "DagRun part_run_3: partition_key='2026-01-03T00:00:00' -> None, "
            "partition_date=2026-01-03T00:00:00+00:00 -> None\n"
            "Cleared partition fields on 2 DagRun(s).\n"
        )

        # Out-of-range run untouched
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        # In-range runs cleared
        for run_id in ("part_run_2", "part_run_3"):
            run = _get_run(run_id)
            assert run.partition_key is None
            assert run.partition_date is None

    def test_dry_run_does_not_modify(self, parser, capsys):
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
        assert captured.out == (
            "DagRun part_run_1: partition_key='2026-01-01T00:00:00' -> None, "
            "partition_date=2026-01-01T00:00:00+00:00 -> None\n"
            "DagRun part_run_2: partition_key='2026-01-02T00:00:00' -> None, "
            "partition_date=2026-01-02T00:00:00+00:00 -> None\n"
            "DagRun part_run_3: partition_key='2026-01-03T00:00:00' -> None, "
            "partition_date=2026-01-03T00:00:00+00:00 -> None\n"
            "Dry run: would clear 3 DagRun(s). No changes written.\n"
        )

        expected = {
            "part_run_1": ("2026-01-01T00:00:00", datetime(2026, 1, 1, tzinfo=pendulum.UTC)),
            "part_run_2": ("2026-01-02T00:00:00", datetime(2026, 1, 2, tzinfo=pendulum.UTC)),
            "part_run_3": ("2026-01-03T00:00:00", datetime(2026, 1, 3, tzinfo=pendulum.UTC)),
        }
        for run_id, (expected_key, expected_date) in expected.items():
            run = _get_run(run_id)
            assert run.partition_key == expected_key
            assert run.partition_date == expected_date

    def test_no_match_prints_message(self, parser, capsys):
        partition_command.clear(
            parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--run-id", "does_not_exist"])
        )
        captured = capsys.readouterr()
        assert captured.out == f"No matching DagRuns found for dag_id={DAG_ID}.\n"

    def test_already_cleared_run_is_skipped(self, parser, capsys):
        with create_session() as session:
            run = session.scalar(select(DagRun).where(DagRun.run_id == "part_run_1"))
            run.partition_key = None
            run.partition_date = None
            session.commit()

        partition_command.clear(
            parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--run-id", "part_run_1"])
        )
        captured = capsys.readouterr()
        assert captured.out == (
            "DagRun part_run_1: already cleared, skipping.\nCleared partition fields on 0 DagRun(s).\n"
        )

    def test_requires_run_id_or_range(self, parser):
        with pytest.raises(SystemExit) as excinfo:
            partition_command.clear(parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID]))
        assert excinfo.value.code == (
            "Specify exactly one of --run-id, --partition-key, or a partition_date range "
            "(--start-date/--end-date or --date)."
        )

    def test_run_id_and_range_mutually_exclusive(self, parser):
        with pytest.raises(SystemExit) as excinfo:
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
        assert excinfo.value.code == (
            "Specify exactly one of --run-id, --partition-key, or a partition_date range "
            "(--start-date/--end-date or --date)."
        )

    def test_prints_before_values(self, parser, capsys):
        partition_command.clear(
            parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--run-id", "part_run_1"])
        )
        captured = capsys.readouterr()
        assert captured.out == (
            "DagRun part_run_1: partition_key='2026-01-01T00:00:00' -> None, "
            "partition_date=2026-01-01T00:00:00+00:00 -> None\n"
            "Cleared partition fields on 1 DagRun(s).\n"
        )

    def test_clear_task_instances_resets_run_and_tis(self, parser, capsys):
        _set_tis_state("part_run_2", TaskInstanceState.SUCCESS)
        before_clear_number = _get_run("part_run_2").clear_number

        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    DAG_ID,
                    "--run-id",
                    "part_run_2",
                    "--clear-task-instances",
                ]
            )
        )
        captured = capsys.readouterr()
        assert captured.out == (
            "DagRun part_run_2: partition_key='2026-01-02T00:00:00' -> None, "
            "partition_date=2026-01-02T00:00:00+00:00 -> None\n"
            "Cleared task instances on 1 DagRun(s) (1 task instance(s)).\n"
            "Cleared partition fields on 1 DagRun(s).\n"
        )

        run_2 = _get_run("part_run_2")
        assert run_2.partition_key is None
        assert run_2.partition_date is None
        assert run_2.state == DagRunState.QUEUED
        assert run_2.clear_number == before_clear_number + 1
        assert all(ti.state is None for ti in _get_tis("part_run_2"))

        # Untargeted run is unaffected.
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        assert run_1.state == DagRunState.SUCCESS

    def test_clear_task_instances_dry_run_makes_no_changes(self, parser, capsys):
        _set_tis_state("part_run_2", TaskInstanceState.SUCCESS)

        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    DAG_ID,
                    "--run-id",
                    "part_run_2",
                    "--clear-task-instances",
                    "--dry-run",
                ]
            )
        )
        captured = capsys.readouterr()
        assert captured.out == (
            "DagRun part_run_2: partition_key='2026-01-02T00:00:00' -> None, "
            "partition_date=2026-01-02T00:00:00+00:00 -> None\n"
            "Dry run: would clear task instances on 1 DagRun(s) (1 task instance(s)).\n"
            "Dry run: would clear 1 DagRun(s). No changes written.\n"
        )

        run_2 = _get_run("part_run_2")
        assert run_2.partition_key == "2026-01-02T00:00:00"
        assert run_2.partition_date == datetime(2026, 1, 2, tzinfo=pendulum.UTC)
        assert run_2.state == DagRunState.SUCCESS
        assert all(ti.state == TaskInstanceState.SUCCESS for ti in _get_tis("part_run_2"))

    def test_clear_task_instances_on_already_cleared_run(self, parser, capsys):
        # Partition fields already None, but the user still wants to re-run the DagRun.
        with create_session() as session:
            run = session.scalar(select(DagRun).where(DagRun.run_id == "part_run_1"))
            run.partition_key = None
            run.partition_date = None
            session.commit()
        _set_tis_state("part_run_1", TaskInstanceState.SUCCESS)

        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    DAG_ID,
                    "--run-id",
                    "part_run_1",
                    "--clear-task-instances",
                ]
            )
        )
        captured = capsys.readouterr()
        assert captured.out == (
            "Cleared task instances on 1 DagRun(s) (1 task instance(s)).\n"
            "Cleared partition fields on 0 DagRun(s).\n"
        )

        run_1 = _get_run("part_run_1")
        assert run_1.state == DagRunState.QUEUED
        assert all(ti.state is None for ti in _get_tis("part_run_1"))

    def test_clear_end_date_includes_same_day_runs(self, parser, dag_maker):
        """A run whose partition_date is mid-day on --end-date must be cleared (thread 4b fix)."""
        # Insert a run with partition_date at 15:00 on 2026-01-02.  Without the end-of-day clamp,
        # the filter is `partition_date <= 2026-01-02T00:00:00`, so 15:00 > midnight would NOT
        # be matched.  With the clamp (<=  2026-01-02T23:59:59.999999) it must be cleared.
        dag_maker.create_dagrun(
            run_id="part_run_2_midday",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 15, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T15:00:00",
        )
        dag_maker.sync_dagbag_to_db()

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
                    "2026-01-02",
                ]
            )
        )
        run_midday = _get_run("part_run_2_midday")
        assert run_midday.partition_key is None
        assert run_midday.partition_date is None
        # Runs outside the range are untouched.
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        run_3 = _get_run("part_run_3")
        assert run_3.partition_key == "2026-01-03T00:00:00"

    def test_clear_streams_runs_without_materialising(self, parser, dag_maker):
        """Clearing 250 partitioned runs clears every one of them (streaming smoke test)."""
        clear_db_runs()
        clear_db_dags()
        n = 250
        with dag_maker(
            "dag_stream_250",
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2024, 1, 1),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        for i in range(n):
            dag_maker.create_dagrun(
                run_id=f"stream_run_{i:04d}",
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=datetime(2024, 1, 1, tzinfo=pendulum.UTC) + pendulum.duration(days=i),
                partition_key=f"2024-key-{i:04d}",
            )
        dag_maker.sync_dagbag_to_db()

        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    "dag_stream_250",
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2025-12-31",
                ]
            )
        )

        with create_session() as session:
            runs = list(
                session.scalars(
                    select(DagRun).where(DagRun.dag_id == "dag_stream_250").order_by(DagRun.run_id)
                )
            )

        cleared_run_ids = [r.run_id for r in runs if r.partition_key is None and r.partition_date is None]
        expected_run_ids = [f"stream_run_{i:04d}" for i in range(n)]
        assert cleared_run_ids == expected_run_ids

        clear_db_runs()
        clear_db_dags()

    def test_clear_task_instances_chunks_at_boundary(self, parser, dag_maker):
        """Exactly DR_CHUNK_SIZE runs → clear_task_instances called once with all TIs."""
        chunk_size = 3
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            "dag_chunk_boundary",
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2024, 1, 1),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        for i in range(chunk_size):
            dag_maker.create_dagrun(
                run_id=f"chunk_run_{i:04d}",
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=datetime(2024, 1, 1, tzinfo=pendulum.UTC) + pendulum.duration(days=i),
                partition_key=f"chunk-key-{i:04d}",
            )
        dag_maker.sync_dagbag_to_db()

        with (
            mock.patch.object(partition_command, "DR_CHUNK_SIZE", chunk_size),
            mock.patch(
                "airflow.cli.commands.partition_command.clear_task_instances", autospec=True
            ) as mock_cti,
        ):
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        "dag_chunk_boundary",
                        "--start-date",
                        "2024-01-01",
                        "--end-date",
                        "2025-12-31",
                        "--clear-task-instances",
                    ]
                )
            )

        # Exactly one call: the flush at >= DR_CHUNK_SIZE triggers mid-loop.
        assert len(mock_cti.mock_calls) == 1
        first_call_tis = mock_cti.mock_calls[0].args[0]
        assert len(first_call_tis) == chunk_size  # == 3 when patched

        clear_db_runs()
        clear_db_dags()

    def test_clear_task_instances_chunks_just_over_boundary(self, parser, dag_maker):
        """DR_CHUNK_SIZE + 1 runs → two clear_task_instances calls (first full, second tail)."""
        chunk_size = 3
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            "dag_chunk_over",
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2024, 1, 1),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        for i in range(chunk_size + 1):
            dag_maker.create_dagrun(
                run_id=f"over_run_{i:04d}",
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=datetime(2024, 1, 1, tzinfo=pendulum.UTC) + pendulum.duration(days=i),
                partition_key=f"over-key-{i:04d}",
            )
        dag_maker.sync_dagbag_to_db()

        with (
            mock.patch.object(partition_command, "DR_CHUNK_SIZE", chunk_size),
            mock.patch(
                "airflow.cli.commands.partition_command.clear_task_instances", autospec=True
            ) as mock_cti,
        ):
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        "dag_chunk_over",
                        "--start-date",
                        "2024-01-01",
                        "--end-date",
                        "2025-12-31",
                        "--clear-task-instances",
                    ]
                )
            )

        # Two calls: first flush at DR_CHUNK_SIZE (3 TIs), tail flush with 1 TI.
        assert len(mock_cti.mock_calls) == 2
        first_call_tis = mock_cti.mock_calls[0].args[0]
        second_call_tis = mock_cti.mock_calls[1].args[0]
        assert len(first_call_tis) == chunk_size
        assert len(second_call_tis) == 1

        clear_db_runs()
        clear_db_dags()

    # ------------------------------------------------------------------
    # --partition-key tests
    # ------------------------------------------------------------------

    def test_clear_by_partition_key(self, parser, capsys):
        partition_command.clear(
            parser.parse_args(
                ["partitions", "clear", "--dag-id", DAG_ID, "--partition-key", "2026-01-02T00:00:00"]
            )
        )
        captured = capsys.readouterr()
        assert captured.out == (
            "DagRun part_run_2: partition_key='2026-01-02T00:00:00' -> None, "
            "partition_date=2026-01-02T00:00:00+00:00 -> None\n"
            "Cleared partition fields on 1 DagRun(s).\n"
        )

        run_2 = _get_run("part_run_2")
        assert run_2.partition_key is None
        assert run_2.partition_date is None
        # Other runs unchanged
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        assert run_1.partition_date == datetime(2026, 1, 1, tzinfo=pendulum.UTC)
        run_3 = _get_run("part_run_3")
        assert run_3.partition_key == "2026-01-03T00:00:00"
        assert run_3.partition_date == datetime(2026, 1, 3, tzinfo=pendulum.UTC)

    def test_clear_by_partition_key_no_match(self, parser, capsys):
        partition_command.clear(
            parser.parse_args(
                ["partitions", "clear", "--dag-id", DAG_ID, "--partition-key", "nonexistent-key"]
            )
        )
        captured = capsys.readouterr()
        assert captured.out == f"No matching DagRuns found for dag_id={DAG_ID}.\n"

        # All runs unchanged
        for run_id, expected_key in [
            ("part_run_1", "2026-01-01T00:00:00"),
            ("part_run_2", "2026-01-02T00:00:00"),
            ("part_run_3", "2026-01-03T00:00:00"),
        ]:
            run = _get_run(run_id)
            assert run.partition_key == expected_key

    def test_partition_key_mutually_exclusive_with_run_id(self, parser):
        with pytest.raises(SystemExit) as excinfo:
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        DAG_ID,
                        "--run-id",
                        "part_run_1",
                        "--partition-key",
                        "foo",
                    ]
                )
            )
        assert excinfo.value.code == (
            "Specify exactly one of --run-id, --partition-key, or a partition_date range "
            "(--start-date/--end-date or --date)."
        )

    def test_partition_key_mutually_exclusive_with_date_range(self, parser):
        with pytest.raises(SystemExit) as excinfo:
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        DAG_ID,
                        "--partition-key",
                        "foo",
                        "--start-date",
                        "2026-01-01",
                    ]
                )
            )
        assert excinfo.value.code == (
            "Specify exactly one of --run-id, --partition-key, or a partition_date range "
            "(--start-date/--end-date or --date)."
        )

    # ------------------------------------------------------------------
    # --date a~b tests
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "cli_args",
        [
            ["--start-date", "2026-01-02", "--end-date", "2026-01-03"],
            ["--date", "2026-01-02~2026-01-03"],
        ],
    )
    def test_date_range_syntax_equivalent_to_start_end(self, parser, capsys, cli_args):
        partition_command.clear(parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, *cli_args]))
        captured = capsys.readouterr()
        assert captured.out == (
            "DagRun part_run_2: partition_key='2026-01-02T00:00:00' -> None, "
            "partition_date=2026-01-02T00:00:00+00:00 -> None\n"
            "DagRun part_run_3: partition_key='2026-01-03T00:00:00' -> None, "
            "partition_date=2026-01-03T00:00:00+00:00 -> None\n"
            "Cleared partition fields on 2 DagRun(s).\n"
        )

    @pytest.mark.parametrize(
        "bad_date",
        [
            "2026-01-01",  # no ~
            "~2026-01-01",  # left side empty
            "2026-01-01~",  # right side empty
            "2026-01-01~not-a-date",  # right side parse failure
        ],
    )
    def test_date_range_syntax_invalid_format(self, parser, bad_date):
        with pytest.raises(SystemExit) as excinfo:
            partition_command.clear(
                parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--date", bad_date])
            )
        assert "--date must be in the form 'a~b'" in str(excinfo.value.code)

    def test_date_range_syntax_mutually_exclusive_with_start_end(self, parser):
        with pytest.raises(SystemExit) as excinfo:
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        DAG_ID,
                        "--date",
                        "2026-01-01~2026-01-02",
                        "--start-date",
                        "2026-01-01",
                    ]
                )
            )
        assert excinfo.value.code == "--date cannot be combined with --start-date / --end-date."

    def test_date_range_end_of_day_clamp(self, parser, dag_maker):
        """A run whose partition_date is mid-day on the --date right side must be cleared."""
        dag_maker.create_dagrun(
            run_id="part_run_2_midday_date",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 15, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T15:00:00",
        )
        dag_maker.sync_dagbag_to_db()

        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    DAG_ID,
                    "--date",
                    "2026-01-02~2026-01-02",
                ]
            )
        )

        run_midday = _get_run("part_run_2_midday_date")
        assert run_midday.partition_key is None
        assert run_midday.partition_date is None
        # Midnight run on same date (start-boundary inclusive) is also cleared.
        run_2 = _get_run("part_run_2")
        assert run_2.partition_key is None
        assert run_2.partition_date is None
        # Runs outside the range are untouched.
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        run_3 = _get_run("part_run_3")
        assert run_3.partition_key == "2026-01-03T00:00:00"
