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
            mock.patch.object(partition_command, "TI_CHUNK_SIZE", ti_cap),
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
            mock.patch.object(partition_command, "TI_CHUNK_SIZE", ti_cap),
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
            mock.patch.object(partition_command, "TI_CHUNK_SIZE", ti_cap),
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
        """Pin mid-loop SELECT IN + slice trigger (partition_command.py L120-132).

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
            mock.patch.object(partition_command, "TI_CHUNK_SIZE", ti_cap),
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

    def test_date_range_end_date_is_literal(self, parser, dag_maker):
        """Pin literal <= semantics: --date right side is used as-is (no end-of-day clamp).

        '2026-01-02' parses to midnight 2026-01-02T00:00:00, so a run at 15:00 on
        the same day has partition_date > the bound and must NOT be cleared.
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

        # Midnight run on exact date is matched (partition_date == bound).
        run_2 = _get_run("part_run_2")
        assert run_2.partition_key is None
        assert run_2.partition_date is None
        # 15:00 > midnight bound — NOT cleared.
        run_midday = _get_run("part_run_2_midday_date")
        assert run_midday.partition_key == "2026-01-02T15:00:00"
        assert run_midday.partition_date == datetime(2026, 1, 2, 15, 0, 0, tzinfo=pendulum.UTC)
        # Runs outside the range are untouched.
        run_1 = _get_run("part_run_1")
        assert run_1.partition_key == "2026-01-01T00:00:00"
        run_3 = _get_run("part_run_3")
        assert run_3.partition_key == "2026-01-03T00:00:00"

    def test_clear_with_datetime_end_date_no_clamp(self, parser, dag_maker):
        """Pin literal <= semantics with a datetime endpoint: runs after the bound are excluded."""
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

        # 10:00 is within the boundary (<=), must be cleared.
        run_h10 = _get_run("part_run_h10")
        assert run_h10.partition_key is None
        assert run_h10.partition_date is None
        # 15:00 exceeds the un-clamped 10:00 boundary, must be untouched.
        run_h15 = _get_run("part_run_h15")
        assert run_h15.partition_key == "2026-01-02T15:00:00"

    def test_clear_hourly_window_via_start_end_date(self, parser, dag_maker):
        """ISO datetime --start-date / --end-date selects only runs within the exact window."""
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

        # 02:00 is before the window start — untouched.
        run_h02 = _get_run("part_run_h02")
        assert run_h02.partition_key == "2026-01-02T02:00:00"
        # 05:00 is inside [03:00, 10:00] — cleared.
        run_h05 = _get_run("part_run_h05")
        assert run_h05.partition_key is None
        assert run_h05.partition_date is None
        # 11:00 is after the window end — untouched.
        run_h11b = _get_run("part_run_h11b")
        assert run_h11b.partition_key == "2026-01-02T11:00:00"

    def test_clear_via_date_range_with_datetime_endpoints(self, parser, dag_maker):
        """--date with ISO datetime endpoints does not clamp the right side."""
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

        # 02:00 is before window start — untouched.
        run_h02b = _get_run("part_run_h02b")
        assert run_h02b.partition_key == "2026-01-02T02:00:00"
        # 05:00 is inside [03:00, 10:00] — cleared.
        run_h05b = _get_run("part_run_h05b")
        assert run_h05b.partition_key is None
        assert run_h05b.partition_date is None
        # 11:00 is after un-clamped 10:00 end — untouched.
        run_h11c = _get_run("part_run_h11c")
        assert run_h11c.partition_key == "2026-01-02T11:00:00"
