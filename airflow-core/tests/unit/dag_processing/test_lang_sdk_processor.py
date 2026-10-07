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
import json
import os
import selectors
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import BinaryIO
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
    _StderrExcerpt,
)
from airflow.dag_processing.processor import DagFileParseRequest, DagFileParsingResult
from airflow.exceptions import UnknownExecutorException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.sdk import DAG, BaseOperator, task
from airflow.sdk._shared.secrets_masker import _secrets_masker as sdk_secrets_masker
from airflow.sdk.api.client import Client
from airflow.sdk.api.datamodels._generated import VariableResponse
from airflow.sdk.exceptions import AirflowRuntimeError
from airflow.sdk.execution_time import supervisor
from airflow.sdk.execution_time.comms import GetVariable, MaskSecret, _RequestFrame
from airflow.sdk.execution_time.supervisor import PsutilTracker
from airflow.sdk.importers import DagSourceCode
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG

from tests_common.test_utils.config import conf_vars
from unit.dag_processing.fake_lang_sdk import (
    FakeCoordinator,
    FakeCoordinatorDagImporter,
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


def _read_fds(fd_dir: Path) -> dict[str, str]:
    fds = {}
    for fd in fd_dir.iterdir():
        # A descriptor can close between listing the directory and reading its link.
        with contextlib.suppress(FileNotFoundError):
            fds[fd.name] = os.readlink(fd)
    return fds


def _is_running(pid: int) -> bool:
    try:
        # Init may not have reaped the killed process yet.
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _stops_running(pid: int, timeout: float = 10) -> bool:
    # SIGKILL is delivered asynchronously, so the process may still run right after the kill.
    deadline = time.monotonic() + timeout
    while _is_running(pid):
        if time.monotonic() > deadline:
            return False
        time.sleep(0.05)
    return True


@contextlib.contextmanager
def _masked_secret(secret: str):
    """Register *secret* with the masker of the parse log, and restore the masker's state afterwards."""
    masker = sdk_secrets_masker()
    patterns, replacer = set(masker.patterns), masker.replacer
    masker.add_mask(secret)
    try:
        yield
    finally:
        masker.patterns, masker.replacer = patterns, replacer


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
        logger_filehandle=MagicMock(spec=BinaryIO),
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

    @patch.object(FakeCoordinatorDagImporter, "get_source_code", autospec=True)
    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_each_dag_gets_its_own_source_keyed_by_dag_id(self, mock_parse_dag, mock_get_source_code, parse):
        """Two Dags from one file get their own ``get_source_code`` call and their own entry."""
        mock_parse_dag.side_effect = play_runtime(
            _reply_with(_serialize_dag("north"), _serialize_dag("south"))
        )
        mock_get_source_code.side_effect = lambda self, definition, dag_id=None: DagSourceCode(
            f"source for {dag_id}", "fake"
        )

        proc = parse()

        assert proc.parsing_result.dag_source_codes == {
            "north": DagSourceCode("source for north", "fake"),
            "south": DagSourceCode("source for south", "fake"),
        }
        assert mock_get_source_code.call_args_list == [call(ANY, ANY, "north"), call(ANY, ANY, "south")]

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

    def test_an_exit_without_a_result_shows_the_stderr_output(self, parse):
        runtime = (
            "import sys; sys.stdout.write('not on stderr\\n'); "
            "sys.stderr.write('panic: Dag \"etl\" is already registered\\n\\ngoroutine 1 [running]:\\n'); "
            "sys.exit(2)"
        )

        proc = parse(argv=[sys.executable, "-c", runtime])

        assert proc.parsing_result.import_errors == {
            "dag.native": "The Lang-SDK runtime exited with code 2 without a parse result. "
            'Its stderr output:\npanic: Dag "etl" is already registered\n\ngoroutine 1 [running]:'
        }

    @pytest.mark.enable_redact
    def test_an_exit_without_a_result_masks_the_secrets_in_stderr(self, parse):
        runtime = "import sys; sys.stderr.write('panic: native-secret-value\\n'); sys.exit(2)"

        with _masked_secret("native-secret-value"):
            proc = parse(argv=[sys.executable, "-c", runtime])

        assert proc.parsing_result.import_errors == {
            "dag.native": "The Lang-SDK runtime exited with code 2 without a parse result. "
            "Its stderr output:\npanic: ***"
        }

    @pytest.mark.parametrize(
        ("mapping", "error"),
        [
            pytest.param(
                None,
                "Dag bundle 'testing' has 2 FakeCoordinator coordinators (first, second). "
                "Map the bundle to one of them in [sdk] dag_bundle_to_coordinator.",
                id="no-entry",
            ),
            pytest.param(
                {"other-bundle": "first"},
                "Dag bundle 'testing' has 2 FakeCoordinator coordinators (first, second). "
                "Map the bundle to one of them in [sdk] dag_bundle_to_coordinator.",
                id="entry-for-another-bundle",
            ),
            pytest.param(
                {"testing": "other"},
                "Dag bundle 'testing' has 2 FakeCoordinator coordinators (first, second). "
                "[sdk] dag_bundle_to_coordinator maps it to 'other', a coordinator of another class. "
                "Move these files to another Dag bundle, or keep one FakeCoordinator.",
                id="entry-of-another-class",
            ),
            pytest.param(
                {"testing": "missing"},
                "Dag bundle 'testing' has 2 FakeCoordinator coordinators (first, second). "
                "[sdk] dag_bundle_to_coordinator maps it to 'missing', which cannot be loaded.",
                id="entry-that-cannot-be-loaded",
            ),
            pytest.param(
                "{not json",
                "Unable to parse [sdk] 'dag_bundle_to_coordinator' as valid json",
                id="not-json",
            ),
            pytest.param(
                '["first"]',
                "[sdk] dag_bundle_to_coordinator must be a JSON object that maps Dag bundle names to "
                "coordinator keys",
                id="not-an-object",
            ),
            pytest.param(
                '{"testing": 1}',
                "[sdk] dag_bundle_to_coordinator must be a JSON object that maps Dag bundle names to "
                "coordinator keys",
                id="not-keys",
            ),
        ],
    )
    def test_a_file_that_several_coordinators_could_parse_is_an_import_error(self, parse, mapping, error):
        with fake_coordinator(
            "first",
            "second",
            dag_bundle_to_coordinator=mapping,
            other_coordinators={"other": "airflow.sdk.execution_time.coordinator.BaseCoordinator"},
        ):
            proc = parse()

        assert proc.parsing_result.serialized_dags == []
        assert proc.parsing_result.import_errors == {
            "dag.native": f"Cannot start the Lang-SDK runtime: InvalidCoordinatorError: {error}"
        }

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
        assert _stops_running(int((tmp_path / "leftover.pid").read_text()))

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
            # The spawned interpreter finds the coordinator and its Dag importer again from its environment.
            monkeypatch.setattr(supervisor, "_should_use_exec", lambda: True)
            monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))
            monkeypatch.setenv("AIRFLOW__SDK__COORDINATORS", conf.get("sdk", "coordinators"))
            monkeypatch.setenv(
                "AIRFLOW__DAG_PROCESSOR__DAG_IMPORTER_CONFIGS",
                json.dumps(
                    [
                        {
                            "classpath": f"{FakeCoordinatorDagImporter.__module__}.FakeCoordinatorDagImporter",
                            "kwargs": {"bundle_name": "testing"},
                        }
                    ]
                ),
            )
        with selectors.DefaultSelector() as selector:
            proc = _start(tmp_path, selector, argv=["/bin/sh", "-c", "exec sleep 30"])
            deadline = time.monotonic() + 30
            while psutil.Process(proc.pid).name() != "sleep":
                assert time.monotonic() < deadline, "the runtime did not start"
                proc._service_subprocess(max_wait_time=0.1)
            fd_dir = Path(f"/proc/{proc.pid}/fd")
            # The process takes its new name during exec, before the dynamic loader has opened and
            # closed the libraries it loads, so wait for such a short-lived descriptor to go away.
            # An inherited descriptor stays open for good.
            settle_deadline = time.monotonic() + 5
            while (fds := _read_fds(fd_dir)).keys() != {"0", "1", "2"} and (
                time.monotonic() < settle_deadline
            ):
                time.sleep(0.05)
            proc.kill(signal.SIGKILL)
            proc.close()

        assert sorted(fds) == ["0", "1", "2"], fds
        assert fds["0"] == "/dev/null"


def _render_excerpt(stderr: bytes) -> str:
    excerpt = _StderrExcerpt()
    for line in stderr.splitlines(keepends=True):
        excerpt.add(line)
    return excerpt.render()


class TestStderrExcerpt:
    @pytest.mark.parametrize(
        ("stderr", "shown"),
        [
            pytest.param(
                b'panic: Dag "etl" is already registered\n\ngoroutine 1 [running]:\n',
                'panic: Dag "etl" is already registered\n\ngoroutine 1 [running]:',
                id="all-lines",
            ),
            pytest.param(
                b"".join(b"line %d\n" % i for i in range(25)),
                "\n".join(
                    [
                        *(f"line {i}" for i in range(5)),
                        "... 5 lines omitted ...",
                        *(f"line {i}" for i in range(10, 25)),
                    ]
                ),
                id="first-and-last-lines",
            ),
            pytest.param(
                b"".join(b"line %d\n" % i for i in range(21)),
                "\n".join(
                    [
                        *(f"line {i}" for i in range(5)),
                        "... 1 line omitted ...",
                        *(f"line {i}" for i in range(6, 21)),
                    ]
                ),
                id="one-line-omitted",
            ),
            pytest.param(b"a" * 1200 + b"\n", "a" * 1000 + "\N{HORIZONTAL ELLIPSIS}", id="long-line"),
            pytest.param(b"\n\npanic: x  \r\n\n", "panic: x", id="blank-lines-and-trailing-spaces"),
            pytest.param(b"\tat frame\n", "\tat frame", id="indented-first-line"),
            pytest.param(
                b"bad \xff \xce\xbb\n",
                "bad \N{REPLACEMENT CHARACTER} \u03bb",
                id="not-utf-8",
            ),
            pytest.param(b"nul\x00byte\n", "nul\N{REPLACEMENT CHARACTER}byte", id="nul"),
            pytest.param(b"\n\n", "", id="only-blank-lines"),
        ],
    )
    def test_render(self, stderr, shown):
        assert _render_excerpt(stderr) == shown

    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        ("secret", "stderr", "shown"),
        [
            pytest.param(
                "native-secret-value",
                b"x" * 990 + b"native-secret-value\n",
                "x" * 990 + "***",
                id="across-the-cut",
            ),
            pytest.param("k" * 1500, b"key " + b"k" * 1500 + b"\n", "key ***", id="longer-than-the-cut"),
        ],
    )
    def test_render_hides_a_secret_before_it_cuts_the_line(self, secret, stderr, shown):
        with _masked_secret(secret):
            assert _render_excerpt(stderr) == shown

    @pytest.mark.enable_redact
    def test_render_hides_a_secret_masked_after_its_line(self):
        excerpt = _StderrExcerpt()
        excerpt.add(b"panic: late-secret-value\n")

        with _masked_secret("late-secret-value"):
            assert excerpt.render() == "panic: ***"


class TestRun:
    @staticmethod
    def _run(tmp_path, **spec) -> DagFileParsingResult:
        return LangSDKDagFileProcessorProcess.run(
            path=write_native_file(tmp_path / "dag.native", **spec),
            bundle_path=tmp_path,
            bundle_name="testing",
            dag_file_rel_path="dag.native",
            logger=structlog.get_logger(),
        )

    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_requests_get_an_error(self, mock_parse_dag, tmp_path):
        def reply(request, comms):
            with pytest.raises(AirflowRuntimeError) as ctx:
                comms.send(GetVariable(key="native_var"))
            description = ctx.value.error.detail["message"]
            return _reply_with(_serialize_dag("native_dag", description=description))(request, comms)

        mock_parse_dag.side_effect = play_runtime(reply)

        result = self._run(tmp_path)

        assert result.serialized_dags[0].data["dag"]["description"] == (
            "GetVariable is answered only in the Dag processor"
        )

    @patch("airflow.sdk.execution_time.request_handlers.mask_secret", autospec=True)
    @patch.object(FakeCoordinator, "parse_dag", autospec=True)
    def test_a_secret_is_masked_without_a_client(self, mock_parse_dag, mock_mask_secret, tmp_path):
        def reply(request, comms):
            comms.send(MaskSecret(value="native-secret", name="native_conn"))
            return _reply_with(_serialize_dag("native_dag"))(request, comms)

        mock_parse_dag.side_effect = play_runtime(reply)

        result = self._run(tmp_path)

        assert [dag.dag_id for dag in result.serialized_dags] == ["native_dag"]
        mock_mask_secret.assert_called_once_with("native-secret", "native_conn")

    @pytest.mark.parametrize("connected", [False, True], ids=["before-connecting", "after-connecting"])
    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True, return_value=1)
    def test_a_parse_past_the_import_timeout_is_killed(self, mock_timeout, tmp_path, connected):
        # A runtime that never connects leaves both listeners open when it is killed.
        fds_before = _get_open_fds()

        with (
            patch.object(
                FakeCoordinator,
                "parse_dag",
                autospec=True,
                side_effect=play_runtime(lambda request, comms: time.sleep(60)),
            )
            if connected
            else contextlib.nullcontext(),
            patch.object(
                LangSDKDagFileProcessorProcess,
                "close",
                autospec=True,
                side_effect=LangSDKDagFileProcessorProcess.close,
            ) as mock_close,
        ):
            result = self._run(tmp_path, argv=["/bin/sh", "-c", "exec sleep 60"])

        assert result.import_errors == {
            "dag.native": f"The Lang-SDK runtime did not parse {tmp_path / 'dag.native'} within 1.0s, "
            "the limit set by [core] dagbag_import_timeout or the get_dagbag_import_timeout policy"
        }
        [proc] = [c.args[0] for c in mock_close.call_args_list]
        assert proc._exit_code == -9
        assert not proc._open_sockets
        assert _get_open_fds() <= fds_before

    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True, return_value=1)
    def test_the_import_timeout_holds_after_the_runtime_exits(self, mock_timeout, tmp_path):
        with patch.object(
            LangSDKDagFileProcessorProcess,
            "close",
            autospec=True,
            side_effect=LangSDKDagFileProcessorProcess.close,
        ) as mock_close:
            # The runtime exits, and the process it leaves behind keeps its output open.
            result = self._run(tmp_path, argv=["/bin/sh", "-c", "sleep 30 & exit 0"])
        [proc] = [c.args[0] for c in mock_close.call_args_list]

        assert result.import_errors == {
            "dag.native": f"The Lang-SDK runtime did not parse {tmp_path / 'dag.native'} within 1.0s, "
            "the limit set by [core] dagbag_import_timeout or the get_dagbag_import_timeout policy"
        }
        assert proc._exit_code == 0
        assert not proc._open_sockets

    @conf_vars({("dag_processor", "dag_file_processor_timeout"): "1"})
    @patch.object(
        FakeCoordinator,
        "_build_parse_dag_command",
        autospec=True,
        side_effect=lambda self, *, path: time.sleep(60),
    )
    def test_the_dag_file_processor_timeout_applies_until_the_import_timeout_is_reported(
        self, mock_build_parse_dag_command, tmp_path
    ):
        result = self._run(tmp_path)

        assert result.import_errors == {
            "dag.native": f"The Lang-SDK runtime did not parse {tmp_path / 'dag.native'} within 1.0s, "
            "the limit set by [dag_processor] dag_file_processor_timeout"
        }


@pytest.mark.parametrize(("configured", "expected"), [(30, 30), (0.5, 0.5), (0, None), (-1, None)])
@patch("airflow.settings.get_dagbag_import_timeout", autospec=True)
def test_only_a_positive_import_timeout_applies(mock_timeout, configured, expected):
    mock_timeout.return_value = configured

    assert _get_import_timeout("/b/dag.native") == expected
    mock_timeout.assert_called_once_with("/b/dag.native")


def _make_process(**kwargs) -> LangSDKDagFileProcessorProcess:
    kwargs.setdefault("process", MagicMock(spec=PsutilTracker))
    kwargs.setdefault("logger_filehandle", MagicMock(spec=BinaryIO))
    return LangSDKDagFileProcessorProcess(
        id=uuid.uuid4(),
        pid=1,
        stdin=MagicMock(spec=socket.socket),
        process_log=MagicMock(spec=FilteringBoundLogger),
        selector=MagicMock(spec=selectors.BaseSelector),
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

    source = proc.parsing_result.dag_source_codes[dag.dag_id]
    assert source.language == "text"
    assert source.source_code.startswith("Cannot read the source of dag.native: [Errno 2] No such file")


@patch.object(DagSerialization, "from_dict", autospec=True, side_effect=DagSerialization.from_dict)
def test_a_dag_is_deserialized_once_to_validate_it_and_apply_the_team_rules(mock_from_dict):
    proc = _make_process()

    proc._handle_request(
        DagFileParsingResult(fileloc="/b/dag.native", serialized_dags=[_serialize_dag("native_dag")]),
        MagicMock(),
        1,
    )

    assert proc.parsing_result.import_errors is None
    mock_from_dict.assert_called_once()


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
def test_a_runtime_that_dies_before_its_parse_request_is_sent_is_not_fatal(mock_send_msg):
    mock_send_msg.side_effect = BrokenPipeError
    proc = _make_process()
    runtime, conn = socket.socketpair()
    with runtime:
        proc._register_comm(conn)

    proc.selector.unregister.assert_called_once_with(conn)
    assert conn not in proc._open_sockets
    assert conn.fileno() == -1


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
    logger_filehandle = MagicMock(spec=BinaryIO)
    proc = _make_process(new_process_group=True, logger_filehandle=logger_filehandle)
    proc._exit_code = 0

    proc.close()
    proc.close()

    assert mock_killpg.call_args_list == ([call(1, signal.SIGKILL)] if killed else [])
    logger_filehandle.close.assert_called()


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
