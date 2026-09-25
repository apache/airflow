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

import fcntl
import os
import signal
from datetime import datetime, timedelta, timezone
from multiprocessing.connection import Connection
from multiprocessing.context import SpawnContext
from multiprocessing.process import BaseProcess
from unittest import mock
from uuid import uuid4

import httpx
import jwt
import pytest
import time_machine
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat

from airflow.dag_processing import executor_manager
from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.dag_processing.executor_manager import ROUTE, ExecutorDagProcessor
from airflow.dag_processing.orchestrator import DiscoveredDefinition, OrchestrationStore, ParseOrchestrator
from airflow.dag_processing.parsing_metadata import MetadataOrchestrationStore
from airflow.executors.workloads import BundleInfo, WorkloadType
from airflow.executors.workloads.parsing import DagDefinitionResult, ParseDagDefinitions

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test
NOW = datetime(2026, 9, 25, tzinfo=timezone.utc)


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "airflow.db"
    path.touch()
    with conf_vars(
        {
            ("database", "sql_alchemy_conn"): f"sqlite:///{path}",
            ("core", "load_examples"): "False",
            ("dag_processor", "parsing_processes"): "1",
            ("dag_processor", "min_file_process_interval"): "0",
        }
    ):
        yield path


@pytest.fixture
def processes():
    return [mock.create_autospec(BaseProcess, instance=True) for _ in range(2)]


@pytest.mark.parametrize(
    "value", ["postgresql://localhost/airflow", "sqlite://", "sqlite:///:memory:", "sqlite:///file?uri=true"]
)
def test_rejects_unsupported_database(value):
    with conf_vars({("database", "sql_alchemy_conn"): value}):
        with pytest.raises(ValueError, match="file-backed SQLite"):
            ExecutorDagProcessor()._get_store_path()


def test_validates_capacity_and_run_count(database):
    with pytest.raises(ValueError, match="num-runs"):
        ExecutorDagProcessor(max_runs=-2)
    with conf_vars({("dag_processor", "parsing_processes"): "0"}):
        with pytest.raises(ValueError, match="positive"):
            ExecutorDagProcessor().run()
    ExecutorDagProcessor(max_runs=0).run()


@mock.patch("airflow.dag_processing.executor_manager.DagBundlesManager", autospec=True)
@pytest.mark.parametrize("selected", [None, ["second"]])
def test_configured_local_bundles(manager, tmp_path, selected):
    bundles = {name: LocalDagBundle(name=name, path=str(tmp_path)) for name in ("first", "second")}
    manager.return_value.get_all_bundle_names.return_value = list(bundles)
    manager.return_value.get_bundle.side_effect = bundles.__getitem__
    processor = ExecutorDagProcessor(bundle_names_to_parse=selected)
    result = processor._get_bundles()
    assert [bundle.name for bundle in result] == (selected or ["first", "second"])
    assert all(bundle.is_initialized for bundle in result)
    manager.return_value.sync_bundles_to_db.assert_called_once_with(deactivate_missing=not selected)


@mock.patch("airflow.dag_processing.executor_manager.DagBundlesManager", autospec=True)
@pytest.mark.parametrize("kind", ["remote", "missing"])
def test_rejects_unsupported_bundle_before_sync(manager, tmp_path, kind):
    bundle = (
        mock.create_autospec(BaseDagBundle, instance=True)
        if kind == "remote"
        else LocalDagBundle(name="missing", path=str(tmp_path / "missing"))
    )
    bundle.name = "unsupported"
    manager.return_value.get_bundle.return_value = bundle
    with pytest.raises(ValueError, match="LocalDagBundle|directory does not exist"):
        ExecutorDagProcessor(bundle_names_to_parse=["unsupported"])._get_bundles()
    manager.return_value.sync_bundles_to_db.assert_not_called()


def test_single_host_lock_is_exclusive(database):
    with database.with_name(database.name + ".parsing.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="Another executor parsing processor"):
            ExecutorDagProcessor().run()


@mock.patch.object(ExecutorDagProcessor, "_get_bundles", autospec=True)
def test_databases_with_same_stem_have_independent_locks(bundles, database):
    other_database = database.with_suffix(".sqlite")
    other_database.touch()

    def get_bundles(processor):
        if bundles.call_count == 2:
            raise ValueError("both locks acquired")
        with conf_vars({("database", "sql_alchemy_conn"): f"sqlite:///{other_database}"}):
            ExecutorDagProcessor().run()

    bundles.side_effect = get_bundles
    with pytest.raises(ValueError, match="both locks acquired"):
        ExecutorDagProcessor().run()


@time_machine.travel(NOW, tick=False)
def test_startup_preserves_unknown_submission(database):
    store = MetadataOrchestrationStore(database)
    orchestrator = ParseOrchestrator(store, route=ROUTE, bundle="test")
    orchestrator.update_inventory(
        BundleInfo(name="test", version=None),
        [DiscoveredDefinition(relative_path="test.py", source_revision="v1")],
    )
    admitted = orchestrator.step()
    store.mark_submitted(admitted.workload_id)
    with pytest.raises(RuntimeError, match="confirmed termination and recovery"):
        ExecutorDagProcessor().run()
    assert store.get_admissions(ROUTE)[0]["state"] == "submitted"
    assert store.get_attempts(admitted.workload_id)[0]["status"] == "pending"
    store.engine.dispose()


@mock.patch.object(ExecutorDagProcessor, "_get_bundles", autospec=True)
@mock.patch.object(MetadataOrchestrationStore, "retire_unsubmitted_reservation", autospec=True)
@time_machine.travel(NOW, tick=False)
def test_startup_aborts_if_unsent_work_enters_submission(retire, bundles, database):
    store = MetadataOrchestrationStore(database)
    orchestrator = ParseOrchestrator(store, route=ROUTE, bundle="test")
    orchestrator.update_inventory(
        BundleInfo(name="test", version=None),
        [DiscoveredDefinition(relative_path="test.py", source_revision="v1")],
    )
    admitted = orchestrator.step()
    bundles.return_value = [LocalDagBundle(name="test", path=str(database.parent))]

    def submit_before_retirement(store, workload_id):
        store.mark_submitted(workload_id)
        return False

    retire.side_effect = submit_before_retirement
    with pytest.raises(RuntimeError, match="entered submission"):
        ExecutorDagProcessor().run()
    assert store.get_admissions(ROUTE)[0]["state"] == "submitted"
    assert store.get_attempts(admitted.workload_id)[0]["status"] == "pending"
    store.engine.dispose()


@mock.patch.object(ExecutorDagProcessor, "_get_bundles", autospec=True, return_value=[])
def test_startup_does_not_dispatch_reserved_work_outside_bundle_selection(bundles, database):
    store = MetadataOrchestrationStore(database)
    orchestrator = ParseOrchestrator(store, route=ROUTE, bundle="excluded")
    orchestrator.update_inventory(
        BundleInfo(name="excluded", version=None),
        [DiscoveredDefinition(relative_path="test.py", source_revision="v1")],
    )
    orchestrator.step()
    with pytest.raises(RuntimeError, match="outside this selection"):
        ExecutorDagProcessor().run()
    assert store.get_admissions(ROUTE)[0]["state"] == "reserved"
    store.engine.dispose()


@mock.patch("airflow.dag_processing.executor_manager.multiprocessing.get_context", autospec=True)
@mock.patch("airflow.dag_processing.executor_manager._wait_for_api", autospec=True)
@mock.patch.object(ExecutorDagProcessor, "_get_bundles", autospec=True)
@mock.patch.object(ExecutorDagProcessor, "_run_loop", autospec=True)
@pytest.mark.parametrize("fail_at", [None, "api", "loop"])
@pytest.mark.parametrize("reserved", [False, True])
def test_command_owns_and_cleans_up_children(
    loop, bundles, wait, context, database, processes, fail_at, reserved
):
    api, runner = processes
    spawn = mock.create_autospec(SpawnContext, instance=True)
    context.return_value = spawn
    spawn.Process.side_effect = processes
    receiver, sender = (mock.create_autospec(Connection, instance=True) for _ in range(2))
    spawn.Pipe.return_value = receiver, sender
    bundles.return_value = [LocalDagBundle(name="test", path=str(database.parent))]
    if reserved:
        store = MetadataOrchestrationStore(database)
        orchestrator = ParseOrchestrator(store, route=ROUTE, bundle="test")
        orchestrator.update_inventory(
            BundleInfo(name="test", version=None),
            [DiscoveredDefinition(relative_path="old.py", source_revision="old")],
        )
        workload_id = orchestrator.step().workload_id
    old_handler = signal.getsignal(signal.SIGTERM)
    if fail_at:
        (wait if fail_at == "api" else loop).side_effect = RuntimeError("injected startup failure")
        with pytest.raises(RuntimeError, match="injected startup failure"):
            ExecutorDagProcessor(max_runs=1).run()
    else:
        ExecutorDagProcessor(max_runs=1).run()
        assert loop.call_args.args[3] == 1
    api.start.assert_called_once()
    api.terminate.assert_called_once()
    api.close.assert_called_once()
    if fail_at == "api":
        runner.start.assert_not_called()
    else:
        runner.start.assert_called_once()
        runner.close.assert_called_once()
        receiver.close.assert_called()
        sender.close.assert_called()
        assert spawn.Process.call_args.kwargs["args"][3] == {
            "test": {"path": str(database.parent), "version": None}
        }
    assert signal.getsignal(signal.SIGTERM) == old_handler
    if reserved:
        assert store.get_admissions(ROUTE) == []
        assert store.get_attempts(workload_id)[0]["status"] == "retired"
        store.engine.dispose()


def complete_local_batch(store, admission):
    workload = ParseDagDefinitions.model_validate(admission["manifest"] | {"token": "fixture"})
    execution = uuid4()
    store.mark_submitted(workload.workload_id)
    for definition in workload.definitions:
        store.claim(workload.workload_id, definition.attempt_id, execution)
        store.accept_result(
            workload.workload_id,
            definition.attempt_id,
            execution,
            DagDefinitionResult(
                attempt_id=definition.attempt_id,
                relative_path=definition.relative_path,
                source_revision=definition.source_revision,
                outcome="success",
                duration_seconds=0,
            ),
        )
    store.retire_and_replace(
        workload.workload_id,
        termination={
            "kind": "confirmed_worker_termination",
            "workload_id": str(workload.workload_id),
            "execution_ids": [str(execution)],
            "evidence": {"fixture": "exited"},
        },
        start_deadline=NOW + timedelta(seconds=20),
        stop_deadline=NOW + timedelta(seconds=30),
    )


@mock.patch("airflow.dag_processing.executor_manager.time.sleep", autospec=True)
@mock.patch("airflow.dag_processing.executor_manager.discover_python_bundle", autospec=True)
def test_busy_first_bundle_cannot_starve_other_bundle(discover, sleep, database, processes):
    discover.return_value = [DiscoveredDefinition(relative_path="dag.py", source_revision="same")]
    bundles = [LocalDagBundle(name=name, path=str(database.parent)) for name in ("first", "second")]
    processor = ExecutorDagProcessor()
    store = OrchestrationStore(database)
    dispatched = []
    with time_machine.travel(NOW, tick=False) as clock:

        def tick(_):
            for admission in store.get_admissions(ROUTE):
                dispatched.append(admission["manifest"]["bundle_info"]["name"])
                complete_local_batch(store, admission)
            clock.shift(timedelta(seconds=1))
            if len(dispatched) == 4:
                processor.terminate()

        sleep.side_effect = tick
        processor._run_loop(store, bundles, 1, processes)
    assert dispatched == ["first", "second", "first", "second"]


@mock.patch("airflow.dag_processing.executor_manager.time.sleep", autospec=True)
@mock.patch("airflow.dag_processing.executor_manager.discover_python_bundle", autospec=True)
def test_finite_runs_do_not_repeat_finished_definitions_while_other_batch_runs(
    discover, sleep, database, processes
):
    discover.return_value = [
        DiscoveredDefinition(relative_path=f"{index:02}.py", source_revision="same") for index in range(11)
    ]
    bundle = LocalDagBundle(name="test", path=str(database.parent))
    processor = ExecutorDagProcessor(max_runs=1)
    store = OrchestrationStore(database)
    ticks = 0
    with time_machine.travel(NOW, tick=False) as clock:

        def tick(_):
            nonlocal ticks
            ticks += 1
            for admission in store.get_admissions(ROUTE):
                if ticks >= 3 or len(admission["manifest"]["definitions"]) == 1:
                    complete_local_batch(store, admission)
            clock.shift(timedelta(seconds=1))
            assert ticks < 10, "Finite parsing never finished"

        sleep.side_effect = tick
        processor._run_loop(store, [bundle], 2, processes)
    assert [row["accepted_count"] for row in store.get_sources(ROUTE, "test")] == [1] * 11


@pytest.mark.parametrize("alive", [[False, False], [True, False], [True, True]])
def test_shutdown_joins_or_escalates_without_releasing_admissions(processes, alive):
    process = processes[0]
    process.is_alive.side_effect = alive
    executor_manager._stop_process(process, graceful=True)
    assert process.terminate.call_count == int(alive[0])
    assert process.kill.call_count == int(alive[1])
    process.close.assert_called_once()


@mock.patch("airflow.dag_processing.executor_manager.time.sleep", autospec=True)
@mock.patch("airflow.dag_processing.executor_manager.httpx.Client", autospec=True)
def test_api_readiness_retries_transport_failure(client, sleep, processes):
    client.return_value.__enter__.return_value.get.side_effect = [
        httpx.ConnectError("starting"),
        httpx.Response(503),
        httpx.Response(200),
    ]
    executor_manager._wait_for_api("http://127.0.0.1:1", processes[0])
    assert sleep.call_count == 2


@mock.patch("airflow.dag_processing.executor_manager.time.monotonic", autospec=True, side_effect=[0, 31])
def test_api_readiness_is_bounded(clock, processes):
    with pytest.raises(TimeoutError, match="did not become ready"):
        executor_manager._wait_for_api("http://127.0.0.1:1", processes[0])


def test_api_startup_crash_fails_promptly(processes):
    processes[0].is_alive.return_value = False
    with pytest.raises(RuntimeError, match="exited during startup"):
        executor_manager._wait_for_api("http://127.0.0.1:1", processes[0])


@mock.patch("airflow.dag_processing.executor_manager.LocalParsingRunner", autospec=True)
@mock.patch("airflow.dag_processing.executor_manager.LocalExecutor", autospec=True)
@mock.patch.dict(os.environ)
@time_machine.travel(NOW, tick=False)
@pytest.mark.parametrize("fails", [False, True])
def test_runner_configures_local_execution_and_scoped_tokens(executor, runner, database, fails):
    stop = mock.create_autospec(Connection, instance=True)
    stop.poll.side_effect = [False, True]
    key = Ed25519PrivateKey.generate()
    if fails:
        runner.return_value.tick.side_effect = ValueError("injected")
    args = (
        str(database),
        key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()),
        stop,
        {"test": {"path": "/local", "version": None}},
        "http://127.0.0.1:1234",
        2,
        "/logs",
    )
    if fails:
        with pytest.raises(ValueError, match="injected"):
            executor_manager._run_executor(*args)
    else:
        executor_manager._run_executor(*args)
    executor.assert_called_once_with(parallelism=2)
    assert executor.return_value.supported_workload_types == frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    assert os.environ["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] == "http://127.0.0.1:1234/execution/"
    assert os.environ["AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE"] == "1"
    token = runner.call_args.kwargs["token_issuer"](
        {
            "workload_id": "batch",
            "definitions": [{"attempt_id": "attempt"}],
            "stop_deadline": (NOW + timedelta(seconds=800)).isoformat(),
        }
    )
    claims = jwt.decode(token, key.public_key(), algorithms=["EdDSA"], audience="dag-parsing-poc")
    assert claims["sub"] == "batch"
    assert claims["attempt_ids"] == ["attempt"]
    assert claims["exp"] == NOW.timestamp() + 860
    runner.return_value.close.assert_called_once()
    stop.close.assert_called_once()


@mock.patch("airflow.dag_processing.executor_manager.create_app", autospec=True)
@mock.patch("airflow.dag_processing.executor_manager.uvicorn", autospec=True)
def test_api_enables_atomic_metadata_and_orchestration(uvicorn, create, tmp_path):
    listener = mock.create_autospec(executor_manager.socket.socket, instance=True)
    executor_manager._serve_api("state.sqlite", tmp_path / "public.pem", listener)
    create.assert_called_once_with(
        "state.sqlite", tmp_path / "public.pem", persist_metadata=True, orchestrated=True
    )
    uvicorn.Server.return_value.run.assert_called_once_with(sockets=[listener])


@time_machine.travel(NOW, tick=False)
def test_loop_fails_on_unreconciled_deadline(database, processes):
    store = OrchestrationStore(database)
    orchestrator = ParseOrchestrator(store, route=ROUTE, bundle="test")
    orchestrator.update_inventory(
        BundleInfo(name="test", version=None),
        [DiscoveredDefinition(relative_path="test.py", source_revision="v1")],
    )
    admitted = orchestrator.step()
    store.mark_submitted(admitted.workload_id)
    with time_machine.travel(NOW + timedelta(seconds=500), tick=False):
        with pytest.raises(RuntimeError, match="admission remains reserved"):
            ExecutorDagProcessor()._run_loop(store, [], 1, processes)
    assert store.get_admissions(ROUTE)[0]["state"] == "submitted"


def test_loop_checks_children_and_shutdown(database, processes):
    processor = ExecutorDagProcessor()
    processes[0].is_alive.return_value = False
    with pytest.raises(RuntimeError, match="exited unexpectedly"):
        processor._run_loop(OrchestrationStore(database), [], 1, processes)
    processor._handle_signal(signal.SIGTERM, None)
    processor._run_loop(OrchestrationStore(database), [], 1, processes)
    processor.end()


@mock.patch("airflow.dag_processing.executor_manager.time.sleep", autospec=True)
@pytest.mark.parametrize("max_runs", [1, 2, -1])
def test_loop_waits_for_current_invocation_outcomes_and_releases(sleep, database, processes, max_runs):
    source = database.parent / "dags"
    source.mkdir()
    (source / "dag.py").write_text("from airflow.sdk import DAG\ndag = DAG('example', schedule=None)\n")
    bundle = LocalDagBundle(name="example", path=str(source))
    store = OrchestrationStore(database)
    processor = ExecutorDagProcessor(max_runs=max_runs)
    processor.heartbeat = mock.create_autospec(lambda: None)
    with time_machine.travel(NOW, tick=False) as clock:

        def finish_batch(_):
            clock.shift(timedelta(seconds=1))
            for admission in store.get_admissions(ROUTE):
                workload = ParseDagDefinitions.model_validate(admission["manifest"] | {"token": "fixture"})
                if not store.get_results(workload.workload_id):
                    execution_id = uuid4()
                    definition = workload.definitions[0]
                    store.mark_submitted(workload.workload_id)
                    store.claim(workload.workload_id, definition.attempt_id, execution_id)
                    store.accept_result(
                        workload.workload_id,
                        definition.attempt_id,
                        execution_id,
                        DagDefinitionResult(
                            attempt_id=definition.attempt_id,
                            relative_path=definition.relative_path,
                            source_revision=definition.source_revision,
                            outcome="success",
                            duration_seconds=0,
                        ),
                    )
                    continue
                execution_id = store.get_attempts(workload.workload_id)[0]["execution_id"]
                store.retire_and_replace(
                    workload.workload_id,
                    termination={
                        "kind": "confirmed_worker_termination",
                        "workload_id": str(workload.workload_id),
                        "execution_ids": [str(execution_id)],
                        "evidence": {"fixture": "returned"},
                    },
                    start_deadline=NOW + timedelta(seconds=20),
                    stop_deadline=NOW + timedelta(seconds=30),
                )
            if max_runs == -1 and not store.get_admissions(ROUTE):
                processor.terminate()

        sleep.side_effect = finish_batch
        processor._run_loop(store, [bundle], 1, processes)
        assert store.get_sources(ROUTE, bundle.name)[0]["accepted_count"] == max(1, max_runs)
        assert store.get_admissions(ROUTE) == []
        processor.heartbeat.assert_called()
