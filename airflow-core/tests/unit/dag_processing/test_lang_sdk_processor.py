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

import contextlib
import copy
import os
import selectors
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import ANY, MagicMock, call, patch

import psutil
import pytest
import structlog
from structlog.typing import FilteringBoundLogger

from airflow.configuration import conf
from airflow.dag_processing.lang_sdk_processor import (
    _EXIT_GRACE_PERIOD,
    LangSDKDagFileProcessorProcess,
    LangSDKRuntimeSchemaVersion,
    _get_import_timeout,
)
from airflow.dag_processing.processor import DagFileParseRequest, DagFileParsingResult
from airflow.exceptions import UnknownExecutorException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.sdk import DAG, BaseOperator, task
from airflow.sdk.api.client import Client
from airflow.sdk.api.datamodels._generated import VariableResponse
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.comms import GetVariable, _RequestFrame
from airflow.sdk.execution_time.supervisor import PsutilTracker
from airflow.sdk.importers import DagSourceCode
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG

from tests_common.test_utils.config import conf_vars
from unit.dag_processing.fake_lang_sdk import (
    FakeCoordinator,
    fake_coordinator,
    play_runtime,
    write_native_file,
)

# The oldest supervisor schema version, so the parse request is downgraded.
OLDEST_SCHEMA_VERSION = "2026-06-16"


def _serialize_dag(dag_id: str, description: str | None = None) -> LazyDeserializedDAG:
    with DAG(dag_id, schedule=None, description=description) as dag:
        BaseOperator(task_id="extract")
    return LazyDeserializedDAG(data=DagSerialization.to_dict(dag))


def _reply_with(*dags: LazyDeserializedDAG, **result):
    def reply(request: DagFileParseRequest, comms) -> DagFileParsingResult:
        return DagFileParsingResult(fileloc=request.file, serialized_dags=list(dags), **result)

    return reply


def _get_open_fds() -> set[int]:
    # Without /proc, as on macOS, this is empty, so the fd leak checks pass trivially.
    return {int(fd) for fd in os.listdir("/proc/self/fd")} if os.path.isdir("/proc/self/fd") else set()


def _is_running(pid: int) -> bool:
    try:
        # Init may not have reaped the killed process yet.
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


@pytest.fixture(autouse=True)
def _coordinator():
    with fake_coordinator():
        yield


def _start(tmp_path, selector, *, client: Client | None = None, **spec) -> LangSDKDagFileProcessorProcess:
    return LangSDKDagFileProcessorProcess.start(
        id=uuid.uuid4(),
        path=write_native_file(tmp_path / "dag.native", **spec),
        bundle_path=tmp_path,
        bundle_name="testing",
        dag_file_rel_path="dag.native",
        selector=selector,
        logger=structlog.get_logger(),
        client=client or MagicMock(spec=Client),
    )


@pytest.fixture
def parse(tmp_path):
    """Parse ``dag.native`` as the Dag processor does, and check that nothing is left open."""

    def _parse(**kwargs) -> LangSDKDagFileProcessorProcess:
        fds_before = _get_open_fds()
        with selectors.DefaultSelector() as selector:
            proc = _start(tmp_path, selector, **kwargs)
            deadline = time.monotonic() + 30
            while not proc.is_ready:
                assert time.monotonic() < deadline, "the Lang-SDK parse did not finish"
                proc._service_subprocess(max_wait_time=0.1)
            assert selector.get_map() == {}
            proc.close()
        assert _get_open_fds() <= fds_before
        return proc

    return _parse


def _send_an_invalid_frame(request, comms) -> None:
    comms.socket.sendall(bytes.fromhex("00000003c1c1c1"))
    time.sleep(60)


class TestLangSDKDagFileProcessorProcess:
    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_parses_the_dags_the_runtime_returns(self, mock_parse_dag, parse, tmp_path, cap_structlog):
        mock_parse_dag.side_effect = play_runtime(
            _reply_with(_serialize_dag("native_dag")),
            schema_version=OLDEST_SCHEMA_VERSION,
            log_lines=[{"event": "Parsing the bundle", "level": "info"}],
        )

        proc = parse()

        assert proc.parsing_result.fileloc == os.fspath(tmp_path / "dag.native")
        assert proc.parsing_result.import_errors is None
        assert [dag.dag_id for dag in proc.parsing_result.serialized_dags] == ["native_dag"]
        assert list(proc.parsing_result.dag_source_codes.values()) == [
            DagSourceCode((tmp_path / "dag.native").read_text(), "fake")
        ]
        assert proc._subprocess_schema_version == OLDEST_SCHEMA_VERSION
        assert "Parsing the bundle" in cap_structlog

    @patch("airflow.dag_processing.lang_sdk_processor._is_connection_from_pid", autospec=True)
    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_a_connection_is_used_once_it_is_verified(self, mock_parse_dag, mock_owned, tmp_path):
        mock_parse_dag.side_effect = play_runtime(_reply_with(_serialize_dag("native_dag")))
        mock_owned.return_value = False
        with selectors.DefaultSelector() as selector:
            proc = _start(tmp_path, selector)
            child_stdin = proc.stdin
            deadline = time.monotonic() + 30
            while len(proc._unverified_connections) < 2:
                assert proc.stdin is child_stdin, "an unverified connection was used"
                assert time.monotonic() < deadline, "the runtime did not connect"
                proc._service_subprocess(max_wait_time=0.1)
            assert proc.stdin is child_stdin

            mock_owned.return_value = True
            while not proc.is_ready:
                assert time.monotonic() < deadline, "the Lang-SDK parse did not finish"
                proc._service_subprocess(max_wait_time=0.1)
            proc.close()

        assert [dag.dag_id for dag in proc.parsing_result.serialized_dags] == ["native_dag"]

    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_requests_are_answered_by_the_client(self, mock_parse_dag, parse):
        def reply(request, comms):
            variable = comms.send(GetVariable(key="native_var"))
            return _reply_with(_serialize_dag("native_dag", description=variable.value))(request, comms)

        mock_parse_dag.side_effect = play_runtime(reply)
        client = MagicMock(spec=Client)
        client.variables = MagicMock()
        client.variables.get.return_value = VariableResponse(key="native_var", value="from-db")

        proc = parse(client=client)

        [dag] = proc.parsing_result.serialized_dags
        assert dag.data["dag"]["description"] == "from-db"

    @pytest.mark.parametrize(
        ("change", "error"),
        [
            pytest.param(
                {"max_active_runs": "many"},
                "Dag 'broken_dag' does not match the schema at $.dag.max_active_runs: 'many' is not of type 'number'",
                id="schema",
            ),
            pytest.param(
                {"timetable": {"__type": "no.such.Timetable", "__var": {}}},
                "Dag 'broken_dag' cannot be deserialized: TimetableNotRegistered: "
                "Timetable class 'no.such.Timetable' is not registered",
                id="deserialize",
            ),
        ],
    )
    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_a_dag_that_does_not_validate_is_an_import_error(self, mock_parse_dag, parse, change, error):
        broken = _serialize_dag("broken_dag")
        broken.data["dag"].update(change)
        mock_parse_dag.side_effect = play_runtime(_reply_with(broken, _serialize_dag("good_dag")))

        proc = parse()

        assert [dag.dag_id for dag in proc.parsing_result.serialized_dags] == ["good_dag"]
        [message] = proc.parsing_result.import_errors.values()
        assert message.startswith(f"Cannot load the serialized Dag: {error}")

    @conf_vars(
        {
            ("core", "max_active_tasks_per_dag"): "7",
            ("core", "max_active_runs_per_dag"): "3",
            ("scheduler", "catchup_by_default"): "True",
        }
    )
    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_a_dag_setting_left_unset_is_filled_from_the_config(self, mock_parse_dag, parse):
        dag = _serialize_dag("native_dag")
        del dag.data["dag"]["max_active_tasks"], dag.data["dag"]["catchup"]
        dag.data["dag"]["max_active_runs"] = 16
        mock_parse_dag.side_effect = play_runtime(_reply_with(dag))

        proc = parse()

        [stored] = proc.parsing_result.serialized_dags
        assert proc.parsing_result.import_errors is None
        assert stored.data["dag"]["max_active_tasks"] == 7
        assert stored.data["dag"]["max_active_runs"] == 16
        assert stored.data["dag"]["catchup"] is True

    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_a_dag_with_a_cycle_is_an_import_error(self, mock_parse_dag, parse):
        cyclic = _serialize_dag("cyclic_dag")
        [task] = cyclic.data["dag"]["tasks"]
        task["__var"]["downstream_task_ids"] = ["extract"]
        mock_parse_dag.side_effect = play_runtime(_reply_with(cyclic))

        proc = parse()

        assert proc.parsing_result.serialized_dags == []
        assert proc.parsing_result.import_errors == {
            "dag.native": "Cannot load the serialized Dag: Dag 'cyclic_dag' has a cycle through task 'extract'"
        }

    @pytest.mark.parametrize(
        ("spec", "reply", "error"),
        [
            pytest.param(
                {"command_error": "no runtime"},
                None,
                "Cannot start the Lang-SDK runtime: FileNotFoundError: no runtime",
                id="command-not-resolved",
            ),
            pytest.param(
                {"argv": ["/no/such/runtime"]},
                None,
                "Cannot start the Lang-SDK runtime: FileNotFoundError: "
                "[Errno 2] No such file or directory: '/no/such/runtime'",
                id="exec-failed",
            ),
            pytest.param(
                {"argv": ["/bin/sh", "-c", "exit 3"]},
                None,
                "The Lang-SDK runtime exited with code 3 without a parse result",
                id="exits-before-connecting",
            ),
            pytest.param(
                {},
                lambda request, comms: None,
                "The Lang-SDK runtime exited with code 0 without a parse result",
                id="exits-without-a-result",
            ),
            pytest.param(
                {},
                _send_an_invalid_frame,
                "The Lang-SDK runtime sent an invalid frame: MessagePack data is malformed: "
                "invalid opcode '\\xc1' (byte 0)",
                id="invalid-frame",
            ),
        ],
    )
    def test_a_failed_parse_is_an_import_error(self, parse, spec, reply, error):
        with (
            patch.object(FakeCoordinator, "parse_dag", autospec=True, side_effect=play_runtime(reply))
            if reply
            else contextlib.nullcontext()
        ):
            proc = parse(**spec)

        assert proc.parsing_result.serialized_dags == []
        assert proc.parsing_result.import_errors == {"dag.native": error}

    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_a_message_that_does_not_validate_is_an_import_error(self, mock_parse_dag, parse):
        def reply(request, comms):
            body = {
                "type": "DagFileParsingResult",
                "fileloc": request.file,
                "serialized_dags": [{"data": "not a dict"}],
            }
            comms.socket.sendall(_RequestFrame(id=1, body=body).as_bytes())
            time.sleep(60)

        mock_parse_dag.side_effect = play_runtime(reply)

        proc = parse()

        [message] = proc.parsing_result.import_errors.values()
        assert message.startswith("The Lang-SDK runtime sent a message that does not validate: ")
        assert "DagFileParsingResult.serialized_dags.0.data\n  Input should be a valid dictionary" in message
        assert proc._exit_code == -signal.SIGKILL

    @patch.object(
        FakeCoordinator, "parse_dag", autospec=True, side_effect=play_runtime(_send_an_invalid_frame)
    )
    def test_killing_the_runtime_is_not_reported_as_out_of_memory(self, mock_parse_dag, parse, cap_structlog):
        proc = parse()

        assert proc._exit_code == -signal.SIGKILL
        assert not any("Likely out of memory" in str(entry.get("event")) for entry in cap_structlog.entries)

    @patch("airflow.dag_processing.lang_sdk_processor._EXIT_GRACE_PERIOD", 0.5)
    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_a_runtime_that_runs_on_after_its_result_is_killed(self, mock_parse_dag, parse, cap_structlog):
        def reply(request, comms):
            comms.send(_reply_with(_serialize_dag("native_dag"))(request, comms))
            time.sleep(60)

        mock_parse_dag.side_effect = play_runtime(reply)

        proc = parse()

        assert [dag.dag_id for dag in proc.parsing_result.serialized_dags] == ["native_dag"]
        assert proc._exit_code == -signal.SIGKILL
        assert "The Lang-SDK runtime did not exit after its parse result; killing it" in cap_structlog

    @patch("airflow.dag_processing.lang_sdk_processor._EXIT_GRACE_PERIOD", 0.5)
    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_what_the_runtime_leaves_after_its_result_is_killed(
        self, mock_parse_dag, parse, tmp_path, cap_structlog
    ):
        def reply(request, comms):
            comms.send(_reply_with(_serialize_dag("native_dag"))(request, comms))
            # The leftover inherits the runtime's stdout, so the parse is not done when the runtime exits.
            leftover = subprocess.Popen(["sleep", "60"])
            (tmp_path / "leftover.pid").write_text(str(leftover.pid))

        mock_parse_dag.side_effect = play_runtime(reply)

        proc = parse()

        assert [dag.dag_id for dag in proc.parsing_result.serialized_dags] == ["native_dag"]
        assert proc._exit_code == 0
        assert (
            "The Lang-SDK runtime left processes holding its output after its parse result; killing them"
            in cap_structlog
        )
        assert not _is_running(int((tmp_path / "leftover.pid").read_text()))

    @pytest.mark.parametrize(
        ("policy", "error"),
        [
            pytest.param(
                {"side_effect": RuntimeError("policy bug")}, "RuntimeError: policy bug", id="raises"
            ),
            pytest.param(
                {"return_value": "30"},
                "TypeError: Value (30) from get_dagbag_import_timeout must be int or float",
                id="not-a-number",
            ),
        ],
    )
    def test_a_failing_import_timeout_policy_is_an_import_error(self, parse, policy, error):
        with patch("airflow.settings.get_dagbag_import_timeout", autospec=True, **policy):
            proc = parse()

        assert proc.parsing_result.import_errors == {
            "dag.native": f"Cannot start the Lang-SDK runtime: {error}"
        }

    @pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="reads /proc")
    @pytest.mark.parametrize("use_exec", [False, True], ids=["fork", "spawn"])
    def test_the_runtime_inherits_only_its_standard_streams(self, monkeypatch, tmp_path, use_exec):
        if use_exec:
            # The spawned interpreter finds the coordinator again from its environment.
            monkeypatch.setattr(supervisor, "_should_use_exec", lambda: True)
            monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))
            monkeypatch.setenv("AIRFLOW__SDK__COORDINATORS", conf.get("sdk", "coordinators"))
        with selectors.DefaultSelector() as selector:
            proc = _start(tmp_path, selector, argv=["/bin/sh", "-c", "exec sleep 30"])
            deadline = time.monotonic() + 30
            while psutil.Process(proc.pid).name() != "sleep":
                assert time.monotonic() < deadline, "the runtime did not start"
                proc._service_subprocess(max_wait_time=0.1)
            fd_dir = Path(f"/proc/{proc.pid}/fd")
            fds = {fd.name: os.readlink(fd) for fd in fd_dir.iterdir()}
            proc.kill(signal.SIGKILL)
            proc.close()

        assert sorted(fds) == ["0", "1", "2"]
        assert fds["0"] == "/dev/null"


@pytest.mark.parametrize(("configured", "expected"), [(30, 30), (0.5, 0.5), (0, None), (-1, None)])
@patch("airflow.settings.get_dagbag_import_timeout", autospec=True)
def test_only_a_positive_import_timeout_applies(mock_timeout, configured, expected):
    mock_timeout.return_value = configured

    assert _get_import_timeout("/b/dag.native") == expected
    mock_timeout.assert_called_once_with("/b/dag.native")


def _make_process(**kwargs) -> LangSDKDagFileProcessorProcess:
    kwargs.setdefault("process", MagicMock(spec=PsutilTracker))
    return LangSDKDagFileProcessorProcess(
        id=uuid.uuid4(),
        pid=1,
        stdin=MagicMock(spec=socket.socket),
        process_log=MagicMock(spec=FilteringBoundLogger),
        selector=MagicMock(spec=selectors.BaseSelector),
        client=MagicMock(spec=Client),
        bundle_name="testing",
        dag_file_rel_path="dag.native",
        listeners={},
        parse_request=DagFileParseRequest(
            file="/b/dag.native", bundle_path=Path("/b"), bundle_name="testing"
        ),
        **kwargs,
    )


def test_a_dag_source_that_cannot_be_read_is_a_placeholder():
    proc = _make_process()
    dag = _serialize_dag("native_dag")

    proc._handle_request(DagFileParsingResult(fileloc="/b/dag.native", serialized_dags=[dag]), MagicMock(), 1)

    source = proc.parsing_result.dag_source_codes[dag.data["dag"]["fileloc"]]
    assert source.language == "text"
    assert source.source_code.startswith("Cannot read the source of dag.native: [Errno 2] No such file")


@patch.object(LangSDKDagFileProcessorProcess, "send_msg", autospec=True)
def test_the_first_parse_result_wins(mock_send_msg):
    proc = _make_process()
    first = DagFileParsingResult(fileloc="/b/dag.native", serialized_dags=[_serialize_dag("first")])
    second = DagFileParsingResult(fileloc="/b/dag.native", serialized_dags=[_serialize_dag("second")])

    proc._handle_request(first, MagicMock(), 1)
    proc._handle_request(second, MagicMock(), 2)

    assert [dag.dag_id for dag in proc.parsing_result.serialized_dags] == ["first"]
    assert mock_send_msg.call_args.kwargs["error"].detail == {
        "message": "A parse result was already received"
    }


@pytest.mark.parametrize(
    "invalid_frame",
    [
        pytest.param(bytes.fromhex("00000003c1c1c1"), id="does-not-decode"),
        pytest.param(
            _RequestFrame(
                id=2,
                body={
                    "type": "DagFileParsingResult",
                    "fileloc": "/b/dag.native",
                    "serialized_dags": [{"data": "not a dict"}],
                },
            ).as_bytes(),
            id="does-not-validate",
        ),
    ],
)
@patch.object(LangSDKDagFileProcessorProcess, "_kill_runtime", autospec=True)
@patch.object(LangSDKDagFileProcessorProcess, "send_msg", autospec=True)
def test_an_invalid_message_after_the_parse_result_keeps_it(mock_send_msg, mock_kill_runtime, invalid_frame):
    proc = _make_process()
    result = DagFileParsingResult(fileloc="/b/dag.native", serialized_dags=[_serialize_dag("native_dag")])
    runtime, conn = socket.socketpair()
    with runtime, conn:
        proc._register_comm(conn)
        read_frame, _ = proc.selector.register.call_args.args[2]
        runtime.sendall(_RequestFrame(id=1, body=result.model_dump(mode="json")).as_bytes())
        assert read_frame(conn)
        runtime.sendall(invalid_frame)
        assert not read_frame(conn)

    assert [dag.dag_id for dag in proc.parsing_result.serialized_dags] == ["native_dag"]
    assert proc.parsing_result.import_errors is None
    proc.process_log.warning.assert_called_with(
        "Ignoring an invalid message from the Lang-SDK runtime after its parse result", error=ANY
    )
    mock_kill_runtime.assert_called_once_with(proc)


@patch.object(LangSDKDagFileProcessorProcess, "send_msg", autospec=True)
def test_the_schema_version_is_reported_once(mock_send_msg):
    proc = _make_process()

    proc._handle_request(LangSDKRuntimeSchemaVersion(schema_version=OLDEST_SCHEMA_VERSION), MagicMock(), 1)
    proc._handle_request(LangSDKRuntimeSchemaVersion(schema_version=None), MagicMock(), 2)

    assert proc._runtime_schema_version == OLDEST_SCHEMA_VERSION
    assert mock_send_msg.call_args.kwargs["error"].detail["message"] == "Unhandled request"


@pytest.mark.parametrize(("pid_reused", "killed"), [(False, True), (True, False)])
@patch("airflow.dag_processing.lang_sdk_processor.os.killpg", autospec=True)
@patch("airflow.dag_processing.lang_sdk_processor.psutil.pid_exists", autospec=True)
def test_close_kills_what_an_exited_runtime_left_once(mock_pid_exists, mock_killpg, pid_reused, killed):
    mock_pid_exists.return_value = pid_reused
    proc = _make_process(new_process_group=True)
    proc._exit_code = 0

    proc.close()
    proc.close()

    assert mock_killpg.call_args_list == ([call(1, signal.SIGKILL)] if killed else [])


@patch.object(LangSDKDagFileProcessorProcess, "_signal_subprocess", autospec=True)
def test_killing_a_runtime_that_does_not_exit_waits_a_bounded_time(mock_signal):
    process = MagicMock(spec=psutil.Process)
    process.wait.side_effect = psutil.TimeoutExpired(_EXIT_GRACE_PERIOD)
    proc = _make_process(process=PsutilTracker(process))

    proc._kill_runtime()

    assert proc._exit_code is None
    mock_signal.assert_called_once_with(proc, signal.SIGKILL)
    process.wait.assert_called_once_with(_EXIT_GRACE_PERIOD)


@patch.object(
    ExecutorLoader,
    "lookup_executor_name_by_str",
    autospec=True,
    side_effect=UnknownExecutorException("not configured"),
)
def test_a_dag_with_an_unavailable_executor_is_an_import_error(mock_lookup):
    with DAG("remote_dag", schedule=None) as remote_dag:
        BaseOperator(task_id="extract", executor="no.such.Executor")
    proc = _make_process()

    proc._handle_request(
        DagFileParsingResult(
            fileloc="/b/dag.native",
            serialized_dags=[
                LazyDeserializedDAG(data=DagSerialization.to_dict(remote_dag)),
                _serialize_dag("ok"),
            ],
        ),
        MagicMock(),
        1,
    )

    assert [dag.dag_id for dag in proc.parsing_result.serialized_dags] == ["ok"]
    assert proc.parsing_result.import_errors == {
        "dag.native": "UnknownExecutorException: Task 'extract' specifies executor 'no.such.Executor', "
        "which is not available. Make sure it is listed in your [core] executor configuration, or update "
        "the task's executor to use one of the configured executors."
    }


@conf_vars({("core", "multi_team"): "True"})
@patch("airflow.dag_processing.bundles.manager.DagBundlesManager", autospec=True)
def test_tasks_in_the_default_pool_move_to_the_teams_pool(mock_bundles_manager):
    mock_bundles_manager.return_value._bundle_config = {"testing": MagicMock(team_name="team_a")}
    with DAG("team_dag", schedule=None) as dag:
        BaseOperator(task_id="extract")
        BaseOperator(task_id="load", pool="custom")

        @task
        def fan_out(x): ...

        fan_out.expand(x=[1, 2])
    proc = _make_process()

    proc._handle_request(
        DagFileParsingResult(
            fileloc="/b/dag.native", serialized_dags=[LazyDeserializedDAG(data=DagSerialization.to_dict(dag))]
        ),
        MagicMock(),
        1,
    )

    [stored] = proc.parsing_result.serialized_dags
    assert proc.parsing_result.import_errors is None
    tasks = DagSerialization.from_dict(copy.deepcopy(stored.data)).tasks
    assert {t.task_id: t.pool for t in tasks} == {
        "extract": "default_pool_team_a",
        "load": "custom",
        "fan_out": "default_pool_team_a",
    }
