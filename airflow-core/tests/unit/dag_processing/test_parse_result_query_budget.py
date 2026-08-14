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
"""
Statement budget for persisting Dag parse results.

The Dag processor's manager issues these once per parsed file, so a change adding round trips has
to move a number here and account for it in review.

Counts are calibrated against Postgres and the tests carrying them are marked for it, because
statement counts differ by dialect: ``_update_import_errors`` deletes with a predicate the ORM
cannot evaluate in Python, which costs one statement where ``DELETE ... RETURNING`` exists and two
on MySQL. The sweep test hard-codes only the call count and measures the rest, so it runs anywhere.
"""

from __future__ import annotations

import re
import time
from collections import Counter
from contextlib import contextmanager, suppress
from pathlib import Path
from socket import socket, socketpair
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event
from uuid6 import uuid7

from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager, DagFileStat
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.serialization.serialized_objects import LazyDeserializedDAG

from tests_common.test_utils.db import clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

BUNDLE = "testing"
DAG_FILE = "budget_dags.py"

# Per persistence call, and per Dag in the file.
FIXED_PER_FILE = 10
PER_DAG = 3

SWEEP_FILES = 4
# Calls the manager takes for that sweep: one per file today, 1 if a sweep is ever batched.
SWEEP_CALLS = 4


def _classify(statement: str) -> tuple[str, str]:
    """Reduce a statement to (operation, table) so a failure says what changed, not just by how much."""
    collapsed = " ".join(statement.split()).lower()
    operation = collapsed.split(" ", 1)[0]
    patterns = {
        "select": r"\bfrom\s+([a-z_][a-z0-9_]*)",
        "delete": r"\bfrom\s+([a-z_][a-z0-9_]*)",
        "insert": r"\binto\s+([a-z_][a-z0-9_]*)",
        "update": r"\bupdate\s+([a-z_][a-z0-9_]*)",
    }
    match = re.search(patterns[operation], collapsed) if operation in patterns else None
    return operation, (match.group(1) if match else "?")


@contextmanager
def _count_statements(session):
    counts: Counter[tuple[str, str]] = Counter()

    def _capture(conn, cursor, statement, parameters, context, executemany):
        counts[_classify(statement)] += 1

    bind = session.get_bind()
    event.listen(bind, "before_cursor_execute", _capture)
    try:
        yield counts
    finally:
        event.remove(bind, "before_cursor_execute", _capture)


def _breakdown(counts: Counter[tuple[str, str]]) -> str:
    return "\n".join(f"  {n:>3}  {op.upper():<6} {table}" for (op, table), n in sorted(counts.items()))


@pytest.fixture(autouse=True)
def clean_db():
    yield
    clear_db_serialized_dags()
    clear_db_dags()


@pytest.fixture
def sockets():
    """Socket ends to close on teardown; ``DagFileProcessorProcess.close()`` leaves stdin open."""
    open_sockets: list[socket] = []
    yield open_sockets
    for sock in open_sockets:
        with suppress(OSError):
            sock.close()


def _make_dags(dag_file: Path, dag_ids: list[str], rel_path: str) -> list[LazyDeserializedDAG]:
    # DagCode reads the source off disk; without a real file the Dags fail to serialize and the
    # measured statements stop resembling a real parse.
    dag_file.write_text("# statement budget fixture\n")
    dags = []
    for dag_id in dag_ids:
        dag = DAG(dag_id=dag_id, schedule="@daily")
        EmptyOperator(task_id="task1", dag=dag)
        dag.fileloc = str(dag_file)
        dag.relative_fileloc = rel_path
        dags.append(LazyDeserializedDAG.from_dag(dag))
    return dags


def _measure_call(session, dags: list[LazyDeserializedDAG]) -> Counter[tuple[str, str]]:
    """Count one steady-state call: the warm pass inserts the rows, the counted pass re-reads them."""
    files_parsed = {(BUNDLE, DAG_FILE)}
    errors: dict = {}

    update_dag_parsing_results_in_db(
        BUNDLE, None, dags, errors, 0.1, set(), session, files_parsed=files_parsed
    )
    session.commit()
    assert not errors, f"fixture Dags must serialize cleanly: {errors}"

    with _count_statements(session) as counts:
        update_dag_parsing_results_in_db(
            BUNDLE, None, dags, errors, 0.1, set(), session, files_parsed=files_parsed
        )
        session.flush()
    return counts


@pytest.mark.backend("postgres")
@pytest.mark.parametrize(
    ("n_dags", "expected"),
    [
        pytest.param(1, FIXED_PER_FILE + PER_DAG, id="one-dag"),
        pytest.param(5, FIXED_PER_FILE + 5 * PER_DAG, id="five-dags"),
    ],
)
def test_call_statement_budget(n_dags, expected, session, testing_dag_bundle, tmp_path):
    """What one file's parse result costs to persist."""
    dags = _make_dags(tmp_path / DAG_FILE, [f"budget_dag_{i}" for i in range(n_dags)], DAG_FILE)

    counts = _measure_call(session, dags)

    total = sum(counts.values())
    assert total == expected, (
        f"a {n_dags}-Dag file costs {total} statements, expected {expected} "
        f"({FIXED_PER_FILE} per file + {PER_DAG} per Dag).\n{_breakdown(counts)}"
    )


@pytest.mark.backend("postgres")
def test_per_dag_cost_matches_budget(session, testing_dag_bundle, tmp_path):
    """A change trading fixed cost for per-Dag cost leaves the one-Dag total intact; slope catches it."""
    totals = {}
    for n_dags in (1, 5):
        dags = _make_dags(tmp_path / DAG_FILE, [f"budget_dag_{i}" for i in range(n_dags)], DAG_FILE)
        totals[n_dags] = sum(_measure_call(session, dags).values())
        clear_db_serialized_dags()
        clear_db_dags()

    slope = (totals[5] - totals[1]) / 4
    assert slope == PER_DAG, f"per-Dag cost is now {slope}, expected {PER_DAG}: {totals}"


def _ready_processor(rel_path: str, dag_file: Path, dag_ids: list[str], sockets: list[socket]):
    """A finished parser subprocess, as the manager sees it. Mirrors ``mock_processor`` in test_manager."""
    read_end, write_end = socketpair()
    sockets += [read_end, write_end]
    processor = DagFileProcessorProcess(
        process_log=MagicMock(),
        id=uuid7(),
        pid=1234,
        process=MagicMock(wait=MagicMock(return_value=0)),
        stdin=write_end,
        logger_filehandle=MagicMock(),
        client=MagicMock(),
        bundle_name=BUNDLE,
        dag_file_rel_path=rel_path,
    )
    processor._open_sockets.clear()
    processor.start_time = time.monotonic() - 1
    processor.had_callbacks = False
    processor.parsing_result = DagFileParsingResult(
        fileloc=str(dag_file), serialized_dags=_make_dags(dag_file, dag_ids, rel_path)
    )
    return processor


def _register(manager, sweep_dir: Path, n_files: int, sockets: list[socket], dags_per_file: int) -> None:
    for i in range(n_files):
        rel_path = f"file_{i}.py"
        file = DagFileInfo(bundle_name=BUNDLE, rel_path=Path(rel_path), bundle_path=sweep_dir)
        manager._file_stats.setdefault(file, DagFileStat())
        manager._processors[file] = _ready_processor(
            rel_path, sweep_dir / rel_path, [f"dag_{i}_{d}" for d in range(dags_per_file)], sockets
        )


def _measure_sweep(session, tmp_path: Path, n_files: int, sockets, name: str, dags_per_file=1) -> int:
    """Count a steady-state sweep through ``_collect_results``."""
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None
    sweep_dir = tmp_path / name
    sweep_dir.mkdir()

    # The manager persists on sessions of its own, so release ours rather than contend with them.
    _register(manager, sweep_dir, n_files, sockets, dags_per_file)
    session.commit()
    manager._collect_results()

    # Collecting consumed the processors, so register a second set for the counted sweep.
    _register(manager, sweep_dir, n_files, sockets, dags_per_file)
    with _count_statements(session) as counts:
        manager._collect_results()
    return sum(counts.values())


def test_sweep_pays_fixed_cost_once_per_call(session, testing_dag_bundle, tmp_path, sockets):
    """
    How a sweep scales with the number of persistence calls it takes.

    Batching a sweep into one call moves ``SWEEP_CALLS`` to 1. Both prices are measured here, so the
    assertion holds on any backend.
    """
    one_dag = _measure_sweep(session, tmp_path, 1, sockets, "one")
    two_dags = _measure_sweep(session, tmp_path, 1, sockets, "two", dags_per_file=2)
    per_dag = two_dags - one_dag
    fixed = one_dag - per_dag

    sweep = _measure_sweep(session, tmp_path, SWEEP_FILES, sockets, "sweep")

    expected = SWEEP_CALLS * fixed + SWEEP_FILES * per_dag
    assert sweep == expected, (
        f"a {SWEEP_FILES}-file sweep costs {sweep} statements, expected {expected} "
        f"({SWEEP_CALLS} x {fixed} fixed + {SWEEP_FILES} x {per_dag} per Dag)."
    )
