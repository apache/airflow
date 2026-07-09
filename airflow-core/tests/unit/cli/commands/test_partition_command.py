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

    def test_clear_task_instances_chunks_at_cap(self, parser, dag_maker):
        """2 DRs × 3 TIs/DR = 6 TIs == TI_CHUNK_SIZE → clear_task_instances called once with all 6."""
        ti_cap = 6
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            "dag_chunk_at_cap",
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2024, 1, 1),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
            EmptyOperator(task_id="t2")
            EmptyOperator(task_id="t3")
        for i in range(2):
            dag_maker.create_dagrun(
                run_id=f"at_cap_run_{i:04d}",
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=datetime(2024, 1, 1, tzinfo=pendulum.UTC) + pendulum.duration(days=i),
                partition_key=f"at-cap-key-{i:04d}",
            )
        dag_maker.sync_dagbag_to_db()

        with (
            mock.patch("airflow.models.dagrun._TI_CHUNK_SIZE", ti_cap),
            mock.patch("airflow.models.dagrun.clear_task_instances", autospec=True) as mock_cti,
        ):
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        "dag_chunk_at_cap",
                        "--start-date",
                        "2024-01-01",
                        "--end-date",
                        "2025-12-31",
                        "--clear-task-instances",
                    ]
                )
            )

        # Exactly one call: 6 TIs == cap, all flushed together in the tail.
        assert [len(c.args[0]) for c in mock_cti.mock_calls] == [6]

        clear_db_runs()
        clear_db_dags()

    def test_clear_task_instances_chunks_over_cap(self, parser, dag_maker):
        """3 DRs × 3 TIs/DR = 9 TIs > TI_CHUNK_SIZE=6 → two calls: [6, 3]."""
        ti_cap = 6
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            "dag_chunk_over_cap",
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2024, 1, 1),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
            EmptyOperator(task_id="t2")
            EmptyOperator(task_id="t3")
        for i in range(3):
            dag_maker.create_dagrun(
                run_id=f"over_cap_run_{i:04d}",
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=datetime(2024, 1, 1, tzinfo=pendulum.UTC) + pendulum.duration(days=i),
                partition_key=f"over-cap-key-{i:04d}",
            )
        dag_maker.sync_dagbag_to_db()

        with (
            mock.patch("airflow.models.dagrun._TI_CHUNK_SIZE", ti_cap),
            mock.patch("airflow.models.dagrun.clear_task_instances", autospec=True) as mock_cti,
        ):
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        "dag_chunk_over_cap",
                        "--start-date",
                        "2024-01-01",
                        "--end-date",
                        "2025-12-31",
                        "--clear-task-instances",
                    ]
                )
            )

        # First slice: 6 TIs at cap; tail: 3 remaining TIs.
        assert [len(c.args[0]) for c in mock_cti.mock_calls] == [6, 3]

        clear_db_runs()
        clear_db_dags()

    def test_clear_task_instances_chunks_just_under_cap(self, parser, dag_maker):
        """1 DR × 5 TIs = 5 TIs < TI_CHUNK_SIZE=6 → one tail call with all 5."""
        ti_cap = 6
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            "dag_chunk_under_cap",
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2024, 1, 1),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
            EmptyOperator(task_id="t2")
            EmptyOperator(task_id="t3")
            EmptyOperator(task_id="t4")
            EmptyOperator(task_id="t5")
        dag_maker.create_dagrun(
            run_id="under_cap_run_0000",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2024, 1, 1, tzinfo=pendulum.UTC),
            partition_key="under-cap-key-0000",
        )
        dag_maker.sync_dagbag_to_db()

        with (
            mock.patch("airflow.models.dagrun._TI_CHUNK_SIZE", ti_cap),
            mock.patch("airflow.models.dagrun.clear_task_instances", autospec=True) as mock_cti,
        ):
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        "dag_chunk_under_cap",
                        "--start-date",
                        "2024-01-01",
                        "--end-date",
                        "2025-12-31",
                        "--clear-task-instances",
                    ]
                )
            )

        # 5 TIs < cap of 6: no mid-loop flush triggered, tail sends all 5 in one call.
        assert [len(c.args[0]) for c in mock_cti.mock_calls] == [5]

        clear_db_runs()
        clear_db_dags()

    def test_clear_task_instances_chunks_mid_loop_trigger(self, parser, dag_maker):
        """Pin mid-loop SELECT IN + slice trigger (dagrun.py clear_partition_runs loop body).

        Design: TI_CHUNK_SIZE=3, 1 dag with 2 tasks, 3 DRs (6 TIs total).

        Execution trace:
        - DR0: ti_buffer=[r0], len=1 < 3 -- no mid-loop
        - DR1: ti_buffer=[r0, r1], len=2 < 3 -- no mid-loop
        - DR2: ti_buffer=[r0, r1, r2], len=3 >= 3 -- mid-loop FIRES:
            SELECT IN(r0, r1, r2) -> 6 TIs
            ti_buffer_run_ids.clear() -> []
            ti_carry.extend(6 TIs) -> carry len=6
            while len>=3: slice[3] -> call #1, carry len=3
            while len>=3: slice[3] -> call #2, carry len=0
            while exits
        - tail: ti_buffer empty, carry empty -- no calls
        Expected mock_calls sizes: [3, 3]

        Note: carry-across-SELECT (mid-loop leftover surviving to the next outer
        fetch) cannot arise in a single-dag CLI invocation.  When mid-loop triggers,
        SELECT returns tasks_per_DR * TI_CHUNK_SIZE TIs -- always a TI_CHUNK_SIZE
        multiple -- so there is never a leftover after the inner while loop.
        Carry leftover only arises in the tail flush, which is pinned by
        test_clear_task_instances_chunks_over_cap.
        """
        ti_cap = 3
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            "dag_chunk_mid_loop",
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2024, 1, 1),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
            EmptyOperator(task_id="t2")
        for i in range(3):
            dag_maker.create_dagrun(
                run_id=f"mid_loop_run_{i:04d}",
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=datetime(2024, 1, 1, tzinfo=pendulum.UTC) + pendulum.duration(days=i),
                partition_key=f"mid-loop-key-{i:04d}",
            )
        dag_maker.sync_dagbag_to_db()

        with (
            mock.patch("airflow.models.dagrun._TI_CHUNK_SIZE", ti_cap),
            mock.patch("airflow.models.dagrun.clear_task_instances", autospec=True) as mock_cti,
        ):
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        "dag_chunk_mid_loop",
                        "--start-date",
                        "2024-01-01",
                        "--end-date",
                        "2025-12-31",
                        "--clear-task-instances",
                    ]
                )
            )

        # Mid-loop fires on DR2: two slices of 3 each; tail is empty.
        assert [len(c.args[0]) for c in mock_cti.mock_calls] == [3, 3]

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
        ],
    )
    def test_date_range_syntax_invalid_format(self, parser, bad_date):
        with pytest.raises(SystemExit) as excinfo:
            partition_command.clear(
                parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, "--date", bad_date])
            )
        assert "--date must be in the form 'a~b'" in str(excinfo.value.code)

    @pytest.mark.parametrize(
        ("cli_args", "expected_message"),
        [
            (
                ["--start-date", "2026-01-03", "--end-date", "2026-01-01"],
                "--start-date must be on or before --end-date.",
            ),
            (
                ["--date", "2026-01-03~2026-01-01"],
                "--date: the start of the range ('2026-01-03') must be on or before the end ('2026-01-01').",
            ),
        ],
    )
    def test_inverted_date_window_is_rejected(self, parser, cli_args, expected_message):
        with pytest.raises(SystemExit) as excinfo:
            partition_command.clear(parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, *cli_args]))
        assert excinfo.value.code == expected_message

    def test_equal_start_and_end_date_is_accepted(self, parser, capsys):
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
        captured = capsys.readouterr()
        assert captured.out == (
            "DagRun part_run_2: partition_key='2026-01-02T00:00:00' -> None, "
            "partition_date=2026-01-02T00:00:00+00:00 -> None\n"
            "Cleared partition fields on 1 DagRun(s).\n"
        )

    def test_offset_aware_window_false_rejection_is_accepted(self, parser, dag_maker):
        """Absolute-instant order disagrees with Asia/Taipei-localized wall-clock order.

        --start-date 2026-01-01T00:00:00+00:00 (instant Jan1 00:00Z) is *after* the
        instant of --end-date 2026-01-01T01:00:00+10:00 (Dec31 15:00Z), so a raw
        instant comparison would wrongly flag this as an inverted window. Once each
        bound's wall-clock reading is localized to the Dag's Asia/Taipei timetable
        timezone (matching downstream ``apply_partition_date_window``), the window
        becomes [Dec31 16:00Z, Dec31 17:00Z) -- lower before upper, valid -- and the
        run seeded inside it must be cleared.
        """
        dag_id = "dag_taipei_offset_false_rejection"
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            dag_id,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei"),
            start_date=datetime(2025, 1, 1, tzinfo=pendulum.UTC),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        dag_maker.create_dagrun(
            run_id="in_window_run",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2025, 12, 31, 16, 30, 0, tzinfo=pendulum.UTC),
            partition_key="in-window",
        )
        dag_maker.sync_dagbag_to_db()

        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    dag_id,
                    "--start-date",
                    "2026-01-01T00:00:00+00:00",
                    "--end-date",
                    "2026-01-01T01:00:00+10:00",
                ]
            )
        )

        run = _get_run("in_window_run")
        assert run.partition_key is None
        assert run.partition_date is None

        clear_db_runs()
        clear_db_dags()

    def test_offset_aware_window_false_acceptance_is_rejected(self, parser, dag_maker):
        """Absolute-instant order disagrees with Asia/Taipei-localized wall-clock order.

        --start-date 2026-01-01T05:00:00+00:00 (instant Jan1 05:00Z) is *before* the
        instant of --end-date 2026-01-01T04:00:00-10:00 (Jan1 14:00Z), so a raw
        instant comparison would wrongly accept this window. Once each bound's
        wall-clock reading is localized to the Dag's Asia/Taipei timetable timezone,
        the window is [Dec31 21:00Z, Dec31 20:00Z) -- lower after upper, inverted --
        and clear() must reject it with the same message as the UTC-only case.
        """
        dag_id = "dag_taipei_offset_false_acceptance"
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            dag_id,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei"),
            start_date=datetime(2025, 1, 1, tzinfo=pendulum.UTC),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        dag_maker.sync_dagbag_to_db()

        with pytest.raises(SystemExit) as excinfo:
            partition_command.clear(
                parser.parse_args(
                    [
                        "partitions",
                        "clear",
                        "--dag-id",
                        dag_id,
                        "--start-date",
                        "2026-01-01T05:00:00+00:00",
                        "--end-date",
                        "2026-01-01T04:00:00-10:00",
                    ]
                )
            )
        assert excinfo.value.code == "--start-date must be on or before --end-date."

        clear_db_runs()
        clear_db_dags()

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

    def test_date_range_date_only_endpoints_default_to_midnight(self, parser, dag_maker):
        """Date-only --date endpoints default to local midnight; the time component is honoured.

        '2026-01-02~2026-01-02' resolves to the inclusive window [Jan 2 00:00Z, Jan 2 00:00Z],
        so only the midnight run is cleared.  A 15:00 run on the same calendar day falls
        after the upper bound and is left untouched.
        """
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

        # Midnight run on the date is cleared.
        run_2 = _get_run("part_run_2")
        assert run_2.partition_key is None
        assert run_2.partition_date is None
        # 15:00 is after the Jan 2 00:00Z upper bound → untouched.
        run_midday = _get_run("part_run_2_midday_date")
        assert run_midday.partition_key == "2026-01-02T15:00:00"
        # Runs outside the date range are untouched.
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        run_3 = _get_run("part_run_3")
        assert run_3.partition_key == "2026-01-03T00:00:00"

    def test_clear_end_date_time_component_honoured(self, parser, dag_maker):
        """The time-of-day in --end-date is honoured; runs after it are not cleared.

        --end-date 2026-01-02T10:00:00 gives the inclusive upper bound Jan 2 10:00Z.
        The 10:00 run is at the bound and cleared; the 15:00 run is after it and
        left untouched.
        """
        dag_maker.create_dagrun(
            run_id="part_run_h10",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 10, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T10:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_run_h15",
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
                    "--end-date",
                    "2026-01-02T10:00:00",
                ]
            )
        )

        # 10:00 on Jan 2 is at the inclusive upper bound — cleared.
        run_h10 = _get_run("part_run_h10")
        assert run_h10.partition_key is None
        assert run_h10.partition_date is None
        # 15:00 on Jan 2 is after the upper bound — untouched.
        run_h15 = _get_run("part_run_h15")
        assert run_h15.partition_key == "2026-01-02T15:00:00"

    def test_clear_datetime_inputs_honour_time_window(self, parser, dag_maker):
        """Datetime --start-date / --end-date define a sub-day window; the time part is honoured.

        --start-date 2026-01-02T03:00:00 → lower = Jan 2 03:00Z.
        --end-date   2026-01-02T10:00:00 → upper = Jan 2 10:00Z.
        Only the 05:00 run sits inside [03:00Z, 10:00Z]; the 02:00 and 11:00 runs are outside.
        """
        dag_maker.create_dagrun(
            run_id="part_run_h02",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 2, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T02:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_run_h05",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 5, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T05:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_run_h11b",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 11, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T11:00:00",
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
                    "2026-01-02T03:00:00",
                    "--end-date",
                    "2026-01-02T10:00:00",
                ]
            )
        )

        # Only the 05:00 run is inside [Jan 2 03:00Z, Jan 2 10:00Z] — cleared.
        run_h05 = _get_run("part_run_h05")
        assert run_h05.partition_key is None
        assert run_h05.partition_date is None
        # 02:00 (before lower) and 11:00 (after upper) are outside the window — untouched.
        assert _get_run("part_run_h02").partition_key == "2026-01-02T02:00:00"
        assert _get_run("part_run_h11b").partition_key == "2026-01-02T11:00:00"

    def test_clear_via_date_range_datetime_endpoints_honour_time(self, parser, dag_maker):
        """--date with ISO datetime endpoints honours the time part; only the in-window run clears."""
        dag_maker.create_dagrun(
            run_id="part_run_h02b",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 2, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T02:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_run_h05b",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 5, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T05:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_run_h11c",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 2, 11, 0, 0, tzinfo=pendulum.UTC),
            partition_key="2026-01-02T11:00:00",
        )
        dag_maker.sync_dagbag_to_db()

        # Window is the inclusive [Jan 2 03:00Z, Jan 2 10:00Z].
        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    DAG_ID,
                    "--date",
                    "2026-01-02T03:00:00~2026-01-02T10:00:00",
                ]
            )
        )

        # Only the 05:00 run is inside the window — cleared.
        run_h05b = _get_run("part_run_h05b")
        assert run_h05b.partition_key is None
        assert run_h05b.partition_date is None
        # 02:00 (before lower) and 11:00 (after upper) are untouched.
        assert _get_run("part_run_h02b").partition_key == "2026-01-02T02:00:00"
        assert _get_run("part_run_h11c").partition_key == "2026-01-02T11:00:00"

    TAIPEI_DAG_ID = "test_partitions_clear_taipei_dag"

    @pytest.fixture
    def seeded_taipei_runs(self, dag_maker, setup_partitioned_runs):
        """Seed DagRuns for a Asia/Taipei (UTC+8) CronPartitionTimetable.

        Depends on ``setup_partitioned_runs`` (the class-level fixture) so that its
        ``clear_db_runs()`` runs before — not after — the Taipei runs are seeded.

        Local midnight in Taipei is stored as UTC-8h in partition_date.
        Written as explicit UTC instants so the oracle is independent of the
        timetable under test:

          local 2026-02-18 midnight  → datetime(2026, 2, 17, 16, 0, 0, UTC)
          local 2026-02-19 midnight  → datetime(2026, 2, 18, 16, 0, 0, UTC)
          local 2026-02-20 midnight  → datetime(2026, 2, 19, 16, 0, 0, UTC)  (outside window)
        """
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            self.TAIPEI_DAG_ID,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei"),
            start_date=datetime(2026, 2, 1, tzinfo=pendulum.UTC),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        runs = [
            (
                "taipei_2026_02_18",
                datetime(2026, 2, 17, 16, 0, 0, tzinfo=pendulum.UTC),
                "2026-02-18T00:00:00",
            ),
            (
                "taipei_2026_02_19",
                datetime(2026, 2, 18, 16, 0, 0, tzinfo=pendulum.UTC),
                "2026-02-19T00:00:00",
            ),
            (
                "taipei_2026_02_20",
                datetime(2026, 2, 19, 16, 0, 0, tzinfo=pendulum.UTC),
                "2026-02-20T00:00:00",
            ),
        ]
        for run_id, partition_date, partition_key in runs:
            dag_maker.create_dagrun(
                run_id=run_id,
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=partition_date,
                partition_key=partition_key,
            )
        dag_maker.sync_dagbag_to_db()
        yield
        clear_db_runs()
        clear_db_dags()

    def _get_taipei_run_partition_dates(self) -> dict[str, datetime | None]:
        with create_session() as session:
            runs = session.scalars(select(DagRun).where(DagRun.dag_id == self.TAIPEI_DAG_ID)).all()
        return {r.run_id: r.partition_date for r in runs}

    @pytest.mark.usefixtures("seeded_taipei_runs")
    def test_taipei_lower_bound_selects_correct_partition(self, parser):
        """--start-date 2026-02-19 must match the run stored at 2026-02-18T16Z.

        Without the timezone fix, parsedate("2026-02-19") yields 2026-02-19T00:00:00Z
        under the UTC default timezone.  The old filter compared
        partition_date >= 2026-02-19T00:00Z; the run for local 2026-02-19 is stored
        as 2026-02-18T16:00Z, which is *before* that UTC boundary, so the run would
        be missed.  Resolving through the timetable timezone fixes the off-by-one:
        the run at 2026-02-18T16Z is selected, the earlier run is not.
        """
        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    self.TAIPEI_DAG_ID,
                    "--start-date",
                    "2026-02-19",
                ]
            )
        )

        dates = self._get_taipei_run_partition_dates()
        # 2026-02-19 local midnight stored as 2026-02-18T16Z — must be cleared (partition_date is None).
        assert dates["taipei_2026_02_19"] is None
        # 2026-02-20 local midnight stored as 2026-02-19T16Z — also in window (no upper bound).
        assert dates["taipei_2026_02_20"] is None
        # 2026-02-18 local midnight stored as 2026-02-17T16Z — before the start, must NOT be cleared.
        assert dates["taipei_2026_02_18"] == datetime(2026, 2, 17, 16, 0, 0, tzinfo=pendulum.UTC)

    @pytest.mark.usefixtures("seeded_taipei_runs")
    def test_taipei_upper_bound_at_cap(self, parser):
        """--end-date 2026-02-19 must include the run stored at 2026-02-18T16Z (at-cap).

        The half-open upper bound is 2026-02-20 midnight Taipei = 2026-02-19T16Z, so
        the run for local 2026-02-19 (stored at 2026-02-18T16Z) falls within the window.
        """
        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    self.TAIPEI_DAG_ID,
                    "--end-date",
                    "2026-02-19",
                ]
            )
        )

        dates = self._get_taipei_run_partition_dates()
        # Both 2026-02-18 and 2026-02-19 local dates are within [start, Feb 20 16Z).
        assert dates["taipei_2026_02_18"] is None
        assert dates["taipei_2026_02_19"] is None
        # 2026-02-20 local midnight (stored at 2026-02-19T16Z) equals the upper bound — NOT cleared.
        assert dates["taipei_2026_02_20"] == datetime(2026, 2, 19, 16, 0, 0, tzinfo=pendulum.UTC)

    @pytest.mark.usefixtures("seeded_taipei_runs")
    def test_taipei_upper_bound_over_cap(self, parser):
        """--end-date 2026-02-18 must NOT include the run stored at 2026-02-18T16Z (over-cap).

        The half-open upper bound is 2026-02-19 midnight Taipei = 2026-02-18T16Z, so
        the run for local 2026-02-19 (stored at exactly that UTC instant) falls outside.
        """
        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    self.TAIPEI_DAG_ID,
                    "--end-date",
                    "2026-02-18",
                ]
            )
        )

        dates = self._get_taipei_run_partition_dates()
        # Only the 2026-02-18 local date run is within the window.
        assert dates["taipei_2026_02_18"] is None
        # 2026-02-19 (stored at 2026-02-18T16Z) equals the upper bound — strictly less than, NOT cleared.
        assert dates["taipei_2026_02_19"] == datetime(2026, 2, 18, 16, 0, 0, tzinfo=pendulum.UTC)
        assert dates["taipei_2026_02_20"] == datetime(2026, 2, 19, 16, 0, 0, tzinfo=pendulum.UTC)

    def test_cross_dag_run_id_collision_does_not_clear_other_dag(self, parser, dag_maker):
        """Clearing by run_id only affects the target Dag; a second Dag sharing the same run_id is untouched."""
        shared_run_id = "cross_dag_shared_run"
        dag_id_target = "cli_cross_dag_target"
        dag_id_bystander = "cli_cross_dag_bystander"
        clear_db_runs()
        clear_db_dags()

        with dag_maker(
            dag_id_target,
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2026, 1, 1),
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        dag_maker.create_dagrun(
            run_id=shared_run_id,
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 1, tzinfo=pendulum.UTC),
            partition_key="key-target",
        )

        with dag_maker(
            dag_id_bystander,
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2026, 1, 1),
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        dag_maker.create_dagrun(
            run_id=shared_run_id,
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 1, 1, tzinfo=pendulum.UTC),
            partition_key="key-bystander",
        )

        dag_maker.sync_dagbag_to_db()

        _set_tis_state(shared_run_id, TaskInstanceState.SUCCESS)

        partition_command.clear(
            parser.parse_args(
                [
                    "partitions",
                    "clear",
                    "--dag-id",
                    dag_id_target,
                    "--run-id",
                    shared_run_id,
                    "--clear-task-instances",
                ]
            )
        )

        with create_session() as session:
            run_target = session.scalar(
                select(DagRun).where(DagRun.dag_id == dag_id_target, DagRun.run_id == shared_run_id)
            )
            run_bystander = session.scalar(
                select(DagRun).where(DagRun.dag_id == dag_id_bystander, DagRun.run_id == shared_run_id)
            )
            tis_bystander = list(
                session.scalars(
                    select(TaskInstance).where(
                        TaskInstance.dag_id == dag_id_bystander,
                        TaskInstance.run_id == shared_run_id,
                    )
                )
            )

        assert run_target.partition_key is None
        assert run_target.partition_date is None

        assert run_bystander.partition_key == "key-bystander"
        assert run_bystander.partition_date is not None
        assert all(ti.state == TaskInstanceState.SUCCESS for ti in tis_bystander)

        # Target Dag's TIs must have been reset (clear_task_instances=True).
        with create_session() as session:
            tis_target = list(
                session.scalars(
                    select(TaskInstance).where(
                        TaskInstance.dag_id == dag_id_target,
                        TaskInstance.run_id == shared_run_id,
                    )
                )
            )
        assert all(ti.state is None for ti in tis_target)

        clear_db_runs()
        clear_db_dags()

    @pytest.mark.parametrize(
        "selector_args",
        [
            ["--run-id", "part_run_1"],
            ["--partition-key", "2026-01-01T00:00:00"],
        ],
    )
    def test_run_id_and_partition_key_do_not_call_get_db_dag(self, parser, selector_args):
        """run_id and partition_key selectors must not load the Dag from the DB.

        get_db_dag raises when the Dag has been deleted but DagRuns remain; the
        CLI must not call it for these two selectors so that orphaned runs can
        still be cleared.
        """
        with mock.patch("airflow.cli.commands.partition_command.get_db_dag") as mock_get_db_dag:
            partition_command.clear(
                parser.parse_args(["partitions", "clear", "--dag-id", DAG_ID, *selector_args])
            )
        mock_get_db_dag.assert_not_called()
