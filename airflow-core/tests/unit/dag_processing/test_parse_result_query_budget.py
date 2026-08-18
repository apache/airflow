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
Statement budget for persisting Dag parse results: a change that adds round trips to this path has
to move a number here and account for it in review.

Counts are calibrated against Postgres and the tests carrying them are marked for it, since
statement counts differ by dialect. The sweep test derives its call count from the group cap and
measures its prices, so it runs anywhere.
"""

from __future__ import annotations

import math
import re
import time
from collections import Counter
from contextlib import contextmanager, suppress
from io import BytesIO
from pathlib import Path
from socket import socket, socketpair
from unittest import mock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError
from uuid6 import uuid7

from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.dag_processing.manager import (
    MAX_FILES_PER_PERSISTENCE_GROUP,
    DagFileInfo,
    DagFileProcessorManager,
    DagFileStat,
    FileParseResult,
)
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning, DagWarningType
from airflow.models.errors import ParseImportError
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.api.client import Client
from airflow.sdk.execution_time.supervisor import ProcessTracker
from airflow.serialization.serialized_objects import LazyDeserializedDAG

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

BUNDLE = "testing"
OTHER_BUNDLE = "testing-other"
DAG_FILE = "budget_dags.py"

# Per persistence call, and per Dag in the file. A call leaves the serialized Dag alone while the
# content is unchanged; once the hash has moved and [core] min_serialized_dag_update_interval has
# lapsed it rewrites it, which costs two more statements per Dag and nothing extra per call.
FIXED_PER_CALL = 10
UNCHANGED_PER_DAG = 3
REWRITE_PER_DAG = 5

SWEEP_FILES = 4


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
    clear_db_import_errors()
    clear_db_dags()


@pytest.fixture
def sockets():
    """Socket ends to close on teardown; ``DagFileProcessorProcess.close()`` leaves stdin open."""
    open_sockets: list[socket] = []
    yield open_sockets
    for sock in open_sockets:
        with suppress(OSError):
            sock.close()


def _make_dags(
    dag_file: Path, dag_ids: list[str], rel_path: str, n_tasks: int = 1
) -> list[LazyDeserializedDAG]:
    # DagCode reads the source off disk; without a real file the Dags fail to serialize and the
    # measured statements stop resembling a real parse.
    dag_file.write_text("# statement budget fixture\n")
    dags = []
    for dag_id in dag_ids:
        dag = DAG(dag_id=dag_id, schedule="@daily")
        for t in range(n_tasks):
            EmptyOperator(task_id=f"task{t}", dag=dag)
        dag.fileloc = str(dag_file)
        dag.relative_fileloc = rel_path
        dags.append(LazyDeserializedDAG.from_dag(dag))
    return dags


def _measure_call(
    session, dags: list[LazyDeserializedDAG], counted: list[LazyDeserializedDAG]
) -> Counter[tuple[str, str]]:
    """Count one steady-state call: the first pass inserts the rows, the counted pass re-persists."""
    files_parsed = {(BUNDLE, DAG_FILE)}
    errors: dict = {}

    update_dag_parsing_results_in_db(
        BUNDLE, None, dags, errors, 0.1, set(), session, files_parsed=files_parsed
    )
    session.commit()
    assert not errors, f"fixture Dags must serialize cleanly: {errors}"

    with _count_statements(session) as counts:
        update_dag_parsing_results_in_db(
            BUNDLE, None, counted, errors, 0.1, set(), session, files_parsed=files_parsed
        )
        session.flush()
    return counts


@pytest.mark.backend("postgres")
@pytest.mark.parametrize("n_dags", [1, 5])
@pytest.mark.parametrize(
    ("rewrite", "per_dag"),
    [
        pytest.param(False, UNCHANGED_PER_DAG, id="unchanged"),
        pytest.param(True, REWRITE_PER_DAG, id="rewrite"),
    ],
)
def test_call_statement_budget(rewrite, per_dag, n_dags, session, testing_dag_bundle, tmp_path):
    """What one file's parse result costs to persist, unchanged and rewritten."""
    dag_ids = [f"budget_dag_{i}" for i in range(n_dags)]
    dags = _make_dags(tmp_path / DAG_FILE, dag_ids, DAG_FILE)
    # A moved hash is what sends the call down the write path; the update interval only gates how
    # soon it can get there.
    counted = _make_dags(tmp_path / DAG_FILE, dag_ids, DAG_FILE, n_tasks=2) if rewrite else dags

    with conf_vars({("core", "min_serialized_dag_update_interval"): "0" if rewrite else "30"}):
        counts = _measure_call(session, dags, counted)

    expected = FIXED_PER_CALL + n_dags * per_dag
    total = sum(counts.values())
    assert total == expected, (
        f"a {n_dags}-Dag file costs {total} statements, expected {expected} "
        f"({FIXED_PER_CALL} per call + {per_dag} per Dag).\n{_breakdown(counts)}"
    )


def _ready_processor(rel_path: str, dag_file: Path, dag_ids: list[str], sockets: list[socket]):
    """A finished parser subprocess, as the manager sees it. Mirrors ``mock_processor`` in test_manager."""
    read_end, write_end = socketpair()
    sockets += [read_end, write_end]
    process = MagicMock(spec=ProcessTracker)
    process.wait.return_value = 0
    processor = DagFileProcessorProcess(
        process_log=MagicMock(),
        id=uuid7(),
        pid=1234,
        process=process,
        stdin=write_end,
        logger_filehandle=BytesIO(),
        client=MagicMock(spec=Client),
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

    _register(manager, sweep_dir, n_files, sockets, dags_per_file)
    manager._collect_results()

    # Collecting consumed the processors, so register a second set for the counted sweep.
    _register(manager, sweep_dir, n_files, sockets, dags_per_file)
    with _count_statements(session) as counts:
        manager._collect_results()
    return sum(counts.values())


@pytest.mark.parametrize(
    "n_files",
    [
        pytest.param(SWEEP_FILES, id="one-group"),
        pytest.param(MAX_FILES_PER_PERSISTENCE_GROUP + 1, id="over-the-group-cap"),
    ],
)
def test_sweep_pays_fixed_cost_once_per_call(n_files, session, testing_dag_bundle, tmp_path, sockets):
    """
    How a sweep scales with the number of persistence calls it takes.

    A sweep is persisted a group at a time, so the fixed price is paid once per group rather than
    once per file -- and a sweep past the cap really does split, rather than only being asserted to.
    Both prices are measured here, so the assertion holds on any backend.
    """
    one_dag = _measure_sweep(session, tmp_path, 1, sockets, "one")
    two_dags = _measure_sweep(session, tmp_path, 1, sockets, "two", dags_per_file=2)
    per_dag = two_dags - one_dag
    fixed = one_dag - per_dag

    sweep = _measure_sweep(session, tmp_path, n_files, sockets, f"sweep_{n_files}")

    calls = math.ceil(n_files / MAX_FILES_PER_PERSISTENCE_GROUP)
    expected = calls * fixed + n_files * per_dag
    assert sweep == expected, (
        f"a {n_files}-file sweep costs {sweep} statements, expected {expected} "
        f"({calls} x {fixed} fixed + {n_files} x {per_dag} per Dag)."
    )


def _parse_result(
    tmp_path: Path, dag_id: str, run_duration: float = 0.5, bundle_name: str = BUNDLE
) -> FileParseResult:
    rel_path = f"{dag_id}.py"
    dag_file = tmp_path / rel_path
    return FileParseResult(
        file=DagFileInfo(bundle_name=bundle_name, rel_path=Path(rel_path), bundle_path=tmp_path),
        parsing_result=DagFileParsingResult(
            fileloc=str(dag_file), serialized_dags=_make_dags(dag_file, [dag_id], rel_path)
        ),
        run_duration=run_duration,
        stat=DagFileStat(),
    )


def test_batched_sweep_keeps_each_files_own_parse_duration(session, testing_dag_bundle, tmp_path):
    """Duration is per file, so writing several files together must not level them out."""
    durations = {"sweep_a": 1.5, "sweep_b": 4.25}
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None

    manager.persist_parsing_results(
        [_parse_result(tmp_path, dag_id, d) for dag_id, d in durations.items()], session=session
    )
    session.commit()

    for dag_id, duration in durations.items():
        assert session.get(DagModel, dag_id).last_parse_duration == duration


def test_per_file_override_still_replaces_the_database(tmp_path):
    """Batching past an existing per-file override would send its results to the DB, silently."""
    calls: list[str] = []

    class ApiBackedManager(DagFileProcessorManager):
        def persist_parsing_result(self, *, relative_fileloc, session, **kwargs):
            assert session is not None, "override must be handed a session it did not create"
            calls.append(relative_fileloc)

    manager = ApiBackedManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None
    batch = [_parse_result(tmp_path, "override_a"), _parse_result(tmp_path, "override_b")]

    with mock.patch("airflow.dag_processing.manager.update_dag_parsing_results_in_db") as db_write:
        manager.persist_parsing_results(batch)

    assert calls == ["override_a.py", "override_b.py"]
    db_write.assert_not_called()


def _collect_a_two_file_sweep(manager, tmp_path: Path, sockets, name: str, session) -> mock.MagicMock:
    """Run a sweep of two files all the way through the manager, and report what reached the DB."""
    sweep_dir = tmp_path / name
    sweep_dir.mkdir()
    _register(manager, sweep_dir, 2, sockets, dags_per_file=1)
    # The manager persists on sessions of its own, so release ours rather than contend with them.
    session.commit()

    with mock.patch("airflow.dag_processing.manager.update_dag_parsing_results_in_db") as db_write:
        manager._collect_results()
    return db_write


def test_a_batch_override_is_handed_the_sweep_by_the_manager(session, testing_dag_bundle, tmp_path, sockets):
    """Dispatching to an override proves nothing unless the manager is the one routing through it."""
    seen: list[list[str]] = []

    class BatchApiManager(DagFileProcessorManager):
        def persist_parsing_results(self, results, *, session=None):
            seen.append([str(item.file.rel_path) for item in results])

    manager = BatchApiManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None

    db_write = _collect_a_two_file_sweep(manager, tmp_path, sockets, "batch_override", session)

    assert seen == [["file_0.py", "file_1.py"]], "the whole sweep should arrive in one call"
    db_write.assert_not_called()


def test_the_default_manager_writes_the_sweep_to_the_database(session, testing_dag_bundle, tmp_path, sockets):
    """The negative of the override case: with nothing replaced, a sweep still reaches the DB once."""
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None

    db_write = _collect_a_two_file_sweep(manager, tmp_path, sockets, "default_path", session)

    db_write.assert_called_once()


def _parse_result_with(
    tmp_path: Path,
    dag_id: str,
    *,
    import_errors: dict[str, str] | None = None,
    warnings: list | None = None,
    dag_ids: list[str] | None = None,
) -> FileParseResult:
    rel_path = f"{dag_id}.py"
    dag_file = tmp_path / rel_path
    return FileParseResult(
        file=DagFileInfo(bundle_name=BUNDLE, rel_path=Path(rel_path), bundle_path=tmp_path),
        parsing_result=DagFileParsingResult(
            fileloc=str(dag_file),
            serialized_dags=_make_dags(dag_file, [dag_id] if dag_ids is None else dag_ids, rel_path),
            import_errors=import_errors,
            warnings=warnings,
        ),
        run_duration=0.5,
        stat=DagFileStat(),
    )


def test_batched_sweep_keeps_each_files_import_errors(session, testing_dag_bundle, tmp_path):
    """Merging a sweep must not lose one file's import errors, nor attribute them to another."""
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None

    manager.persist_parsing_results(
        [
            _parse_result_with(tmp_path, "err_a", import_errors={"err_a.py": "boom a"}),
            _parse_result_with(tmp_path, "err_b"),
        ],
        session=session,
    )
    session.commit()

    recorded = {e.filename: e.stacktrace for e in session.scalars(select(ParseImportError))}
    assert recorded == {"err_a.py": "boom a"}


def test_batched_sweep_clears_import_errors_for_files_that_now_parse(session, testing_dag_bundle, tmp_path):
    """A file in the sweep with no errors must have its stale error cleared, not left behind."""
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None

    manager.persist_parsing_results(
        [_parse_result_with(tmp_path, "fixed", import_errors={"fixed.py": "was broken"})],
        session=session,
    )
    session.commit()
    assert session.scalars(select(ParseImportError)).all()

    manager.persist_parsing_results([_parse_result_with(tmp_path, "fixed")], session=session)
    session.commit()

    assert not session.scalars(select(ParseImportError)).all(), "stale error should have been cleared"


def test_batched_sweep_clears_a_stale_error_for_a_file_that_now_defines_no_dags(
    session, testing_dag_bundle, tmp_path
):
    """A file can stop defining Dags altogether, and is still a file the sweep parsed."""
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None

    manager.persist_parsing_results(
        [_parse_result_with(tmp_path, "emptied", import_errors={"emptied.py": "was broken"})],
        session=session,
    )
    session.commit()
    assert session.scalars(select(ParseImportError)).all()

    manager.persist_parsing_results(
        [_parse_result_with(tmp_path, "healthy"), _parse_result_with(tmp_path, "emptied", dag_ids=[])],
        session=session,
    )
    session.commit()

    remaining = {(error.bundle_name, error.filename) for error in session.scalars(select(ParseImportError))}
    assert (BUNDLE, "emptied.py") not in remaining, (
        f"the emptied file was parsed, so its stale error should have been cleared: {remaining}"
    )


def test_a_sweep_writes_each_bundles_files_under_its_own_version(session, testing_dag_bundle, tmp_path):
    """A group carries one bundle's version, so a sweep spanning two must not cross them over."""
    session.add(DagBundleModel(name=OTHER_BUNDLE))
    session.commit()

    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions.update({BUNDLE: "v-testing", OTHER_BUNDLE: "v-other"})
    manager._bundle_version_data.update({BUNDLE: {"sha": "aaa"}, OTHER_BUNDLE: {"sha": "bbb"}})

    manager.persist_parsing_results(
        [
            _parse_result(tmp_path, "in_testing"),
            _parse_result(tmp_path, "in_other", bundle_name=OTHER_BUNDLE),
        ],
        session=session,
    )
    session.commit()

    assert session.get(DagModel, "in_testing").bundle_name == BUNDLE
    assert session.get(DagModel, "in_other").bundle_name == OTHER_BUNDLE

    versions = {version.dag_id: version for version in session.scalars(select(DagVersion))}
    assert versions["in_testing"].bundle_version == "v-testing"
    assert versions["in_other"].bundle_version == "v-other"
    assert versions["in_testing"].version_data == {"sha": "aaa"}
    assert versions["in_other"].version_data == {"sha": "bbb"}


def test_batched_sweep_records_warnings_from_every_file(session, testing_dag_bundle, tmp_path):
    """Warnings are merged across the sweep, so each file's must survive the merge."""
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions[BUNDLE] = None

    def warning_for(dag_id: str) -> dict:
        return {
            "dag_id": dag_id,
            "warning_type": DagWarningType.NONEXISTENT_POOL,
            "message": f"{dag_id} wants a missing pool",
        }

    manager.persist_parsing_results(
        [
            _parse_result_with(tmp_path, "warn_a", warnings=[warning_for("warn_a")]),
            _parse_result_with(tmp_path, "warn_b", warnings=[warning_for("warn_b")]),
        ],
        session=session,
    )
    session.commit()

    assert {w.dag_id for w in session.scalars(select(DagWarning))} == {"warn_a", "warn_b"}


def test_a_failing_group_does_not_discard_one_that_already_succeeded(tmp_path):
    """
    Each group is persisted in its own transaction.

    ``update_dag_parsing_results_in_db`` rolls the session back before retrying an OperationalError.
    Sharing one transaction across groups would let that rollback discard a group already written,
    while its files were still recorded as persisted.
    """
    manager = DagFileProcessorManager(max_runs=1)
    manager._bundle_versions.update({"bundle_a": None, "bundle_b": None})

    def result_in(bundle: str, dag_id: str) -> FileParseResult:
        item = _parse_result_with(tmp_path, dag_id)
        return item._replace(
            file=DagFileInfo(bundle_name=bundle, rel_path=item.file.rel_path, bundle_path=tmp_path)
        )

    good = result_in("bundle_a", "group_good")
    bad = result_in("bundle_b", "group_bad")
    manager._file_stats.update({good.file: DagFileStat(), bad.file: DagFileStat(run_count=2)})

    calls: list[str] = []

    def persist(results, **kwargs):
        bundle = results[0].file.bundle_name
        calls.append(bundle)
        if bundle == "bundle_b":
            raise OperationalError("simulated contention", None, Exception())

    with mock.patch.object(manager, "persist_parsing_results", side_effect=persist):
        manager._persist_sweep([good, bad])

    assert calls == ["bundle_a", "bundle_b"], "each bundle must be persisted by its own call"
    assert manager._file_stats[good.file] is good.stat, "the written group keeps its parse stat"
    assert manager._file_stats[bad.file] is not bad.stat, "the failed group must not claim success"
    assert manager._file_stats[bad.file].run_count == 3
