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
import json
import os
import time
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
import structlog

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import LiteralArgBinding
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.processor import (
    TaskHandlerArtifact,
    TaskHandlerBinding,
    TaskHandlerParseRequest,
    TaskHandlerParsingResult,
)
from airflow.dag_processing.task_handler_processor import (
    LangSDKTaskHandlerProcessorProcess,
    TaskHandlerProbeStopped,
)
from airflow.dag_processing.task_handler_resolution import TaskHandlerResolution, resolve_task_handlers
from airflow.dag_processing.task_handler_validation import StubTask
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator

from unit.dag_processing.fake_task_handler_runtime import (
    FAKE_COORDINATOR,
    LOCAL_BUNDLE,
    FakeCoordinator,
    reply_with_task_handlers,
    task_handler_config,
    write_artifact,
)

if TYPE_CHECKING:
    from pathlib import Path

PROBED = "Stub tasks in etl.py do not match their task handlers:"


def _declare(*task_ids: str) -> dict:
    return {"etl": [{"task_id": task_id, "binding": "positional", "params": []} for task_id in task_ids]}


def _stub(task_id: str, *, queue: str = "fake-queue", relative_fileloc: str = "etl.py", arg_bindings=()):
    return StubTask(
        dag_id="etl",
        task_id=task_id,
        queue=queue,
        relative_fileloc=relative_fileloc,
        arg_bindings=list(arg_bindings),
        is_mapped=False,
    )


def _binding(task_id: str, *, rel_path: str = "etl.artifact", bundle_name: str = "task-handlers"):
    return TaskHandlerBinding(
        dag_id="etl", task_id=task_id, artifact_bundle_name=bundle_name, artifact_rel_path=rel_path
    )


def _answer(path: Path, *, bundle_name: str = "task-handlers") -> TaskHandlerArtifact:
    content = path.read_bytes()
    spec = json.loads(content)
    return TaskHandlerArtifact.model_validate(
        {
            "bundle_name": bundle_name,
            "relative_fileloc": path.name,
            "size_bytes": len(content),
            "cache_digest": spec.get("cache_digest"),
            "task_handlers": spec["task_handlers"],
        }
    )


def _probe(*, path, bundle_path, bundle_name, **kwargs) -> TaskHandlerParsingResult:
    """Answer as the runtime does, from the ``task_handlers`` of the artifact's JSON."""
    request = TaskHandlerParseRequest(file=os.fspath(path), bundle_path=bundle_path, bundle_name=bundle_name)
    return reply_with_task_handlers(request, None)


def _fail_probe(
    *,
    path,
    artifact_rel_path,
    error="The Lang-SDK runtime exited with code 1 without a parse result",
    result_type=TaskHandlerParsingResult,
    **kwargs,
) -> TaskHandlerParsingResult:
    return result_type(fileloc=os.fspath(path), task_handlers={}, import_errors={artifact_rel_path: error})


def _stop_probe(*, path, artifact_rel_path, **kwargs) -> TaskHandlerParsingResult:
    """Answer as a probe the deadline stopped before its runtime answered."""
    return _fail_probe(
        path=path,
        artifact_rel_path=artifact_rel_path,
        error=f"The Lang-SDK runtime did not parse {path} by its deadline",
        result_type=TaskHandlerProbeStopped,
    )


@pytest.fixture
def dag_bundle(tmp_path) -> Path:
    path = tmp_path / "dags"
    path.mkdir()
    return path


@pytest.fixture
def artifacts(tmp_path) -> Path:
    path = tmp_path / "artifacts"
    path.mkdir()
    return path


@pytest.fixture
def configure(dag_bundle, artifacts):
    """Configure coordinators, their queues and the Dag bundles, as ``task_handler_config`` does."""
    with contextlib.ExitStack() as stack:
        yield lambda *args, **kwargs: stack.enter_context(
            task_handler_config(dag_bundle, artifacts, *args, **kwargs)
        )


def _resolve(stub_tasks, dag_bundle, *, known_artifacts=(), deadline=None) -> TaskHandlerResolution:
    return resolve_task_handlers(
        stub_tasks,
        dag_bundle_name="dags",
        dag_bundle_path=dag_bundle,
        known_artifacts=list(known_artifacts),
        deadline=time.monotonic() + 60 if deadline is None else deadline,
        log=structlog.get_logger(),
    )


def _configure_two_coordinators(configure) -> None:
    """Route ``fake-queue`` to ``fake`` and ``other-queue`` to ``other``, both reading ``task-handlers``."""
    kwargs = {"task_handler_bundle_name": "task-handlers"}
    configure(
        {
            "fake": {"classpath": FAKE_COORDINATOR, "kwargs": kwargs},
            "other": {"classpath": FAKE_COORDINATOR, "kwargs": kwargs},
        },
        queue_to_coordinator={"fake-queue": "fake", "other-queue": "other"},
    )


@patch.object(LangSDKTaskHandlerProcessorProcess, "run", autospec=True, side_effect=_probe)
class TestResolveTaskHandlers:
    def test_bindings_name_every_routed_stub_task(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()
        artifact = write_artifact(
            artifacts / "etl.artifact", cache_digest="d1", task_handlers=_declare("extract", "load")
        )
        deadline = time.monotonic() + 60

        resolution = _resolve(
            [_stub("extract"), _stub("load"), _stub("report", queue="default")], dag_bundle, deadline=deadline
        )

        assert resolution == TaskHandlerResolution(
            bindings=[_binding("extract"), _binding("load")],
            probed_artifacts=[_answer(artifact)],
            import_errors={},
        )
        mock_run.assert_called_once_with(
            coordinator="fake",
            path=artifact,
            bundle_path=artifacts,
            bundle_name="task-handlers",
            artifact_rel_path="etl.artifact",
            logger=mock_run.call_args.kwargs["logger"],
            deadline=deadline,
        )
        assert {"event": "Probing a task handler artifact", "path": "etl.artifact"} in cap_structlog
        assert {"event": "Probed a task handler artifact", "path": "etl.artifact"} in cap_structlog

    def test_a_recorded_answer_is_used_without_probing(self, mock_run, configure, dag_bundle, artifacts):
        configure()
        artifact = write_artifact(
            artifacts / "etl.artifact", cache_digest="d1", task_handlers=_declare("extract")
        )

        resolution = _resolve([_stub("extract")], dag_bundle, known_artifacts=[_answer(artifact)])

        assert resolution == TaskHandlerResolution(
            bindings=[_binding("extract")], probed_artifacts=[], import_errors={}
        )
        mock_run.assert_not_called()

    def test_an_artifact_two_coordinators_list_is_probed_once(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        _configure_two_coordinators(configure)
        artifact = write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", "load"))

        resolution = _resolve([_stub("extract"), _stub("load", queue="other-queue")], dag_bundle)

        assert resolution == TaskHandlerResolution(
            bindings=[_binding("extract"), _binding("load")],
            probed_artifacts=[_answer(artifact)],
            import_errors={},
        )
        assert mock_run.call_count == 1
        assert mock_run.call_args.kwargs["coordinator"] == "fake"

    def test_a_failed_probe_is_retried_under_the_next_coordinator_that_lists_it(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        _configure_two_coordinators(configure)
        artifact = write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", "load"))
        mock_run.side_effect = lambda **kwargs: (
            _fail_probe(**kwargs) if kwargs["coordinator"] == "fake" else _probe(**kwargs)
        )

        resolution = _resolve([_stub("extract"), _stub("load", queue="other-queue")], dag_bundle)

        assert resolution == TaskHandlerResolution(
            bindings=[_binding("extract"), _binding("load")],
            probed_artifacts=[_answer(artifact)],
            import_errors={},
        )
        assert [c.kwargs["coordinator"] for c in mock_run.call_args_list] == ["fake", "other"]

    def test_a_failed_probe_is_named_for_the_coordinator_it_failed_under(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        _configure_two_coordinators(configure)
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", "load"))
        mock_run.side_effect = lambda **kwargs: _fail_probe(
            error=f"{kwargs['coordinator']} cannot run it", **kwargs
        )

        resolution = _resolve(
            [_stub("extract"), _stub("load", queue="other-queue", relative_fileloc="other.py")], dag_bundle
        )

        no_handler = "no artifact in Dag bundle 'task-handlers' registers it; no answer from 'etl.artifact'"
        assert resolution == TaskHandlerResolution(
            bindings=None,
            probed_artifacts=[],
            import_errors={
                "etl.py": f"{PROBED}\n- Dag 'etl', task 'extract': {no_handler} (probe failed: fake cannot run it)",
                "other.py": "Stub tasks in other.py do not match their task handlers:\n"
                f"- Dag 'etl', task 'load': {no_handler} (probe failed: other cannot run it)",
            },
        )
        assert [c.kwargs["coordinator"] for c in mock_run.call_args_list] == ["fake", "other"]

    def test_probed_answers_are_returned_when_validation_fails(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        configure()
        artifact = write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract"))

        resolution = _resolve([_stub("extract"), _stub("load")], dag_bundle)

        assert resolution == TaskHandlerResolution(
            bindings=None,
            probed_artifacts=[_answer(artifact)],
            import_errors={
                "etl.py": f"{PROBED}\n- Dag 'etl', task 'load': no artifact in Dag bundle 'task-handlers' registers it"
            },
        )

    @pytest.mark.parametrize(
        ("task_id", "import_errors"),
        [
            pytest.param("extract", {}, id="handler-found"),
            pytest.param(
                "load",
                {
                    "etl.py": f"{PROBED}\n- Dag 'etl', task 'load': no artifact in Dag bundle 'task-handlers' "
                    "registers it; no answer from 'bad.artifact' (bad.artifact is not executable)"
                },
                id="handler-missing",
            ),
        ],
    )
    def test_a_rejected_candidate_is_ignored_and_named_when_a_handler_is_missing(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog, task_id, import_errors
    ):
        configure()
        write_artifact(artifacts / "bad.artifact", listing_error="bad.artifact is not executable")
        good = write_artifact(artifacts / "good.artifact", task_handlers=_declare("extract"))

        resolution = _resolve([_stub(task_id)], dag_bundle)

        assert resolution.import_errors == import_errors
        assert resolution.probed_artifacts == [_answer(good)]
        assert [c.kwargs["artifact_rel_path"] for c in mock_run.call_args_list] == ["good.artifact"]
        assert {
            "event": "Ignoring a task handler artifact its coordinator cannot probe",
            "path": "bad.artifact",
            "error": "bad.artifact is not executable",
        } in cap_structlog

    @pytest.mark.parametrize(
        ("task_id", "import_errors"),
        [
            pytest.param("extract", {}, id="handler-found"),
            pytest.param(
                "load",
                {
                    "etl.py": f"{PROBED}\n- Dag 'etl', task 'load': no artifact in Dag bundle 'task-handlers' "
                    "registers it; no answer from 'bad.artifact' "
                    "(probe failed: The Lang-SDK runtime exited with code 1 without a parse result)"
                },
                id="handler-missing",
            ),
        ],
    )
    def test_a_failed_probe_is_ignored_and_named_when_a_handler_is_missing(
        self, mock_run, configure, dag_bundle, artifacts, task_id, import_errors
    ):
        configure()
        write_artifact(artifacts / "bad.artifact")
        good = write_artifact(artifacts / "good.artifact", task_handlers=_declare("extract"))
        mock_run.side_effect = lambda **kwargs: (
            _fail_probe(**kwargs) if kwargs["artifact_rel_path"] == "bad.artifact" else _probe(**kwargs)
        )

        resolution = _resolve([_stub(task_id)], dag_bundle)

        assert resolution.import_errors == import_errors
        assert resolution.probed_artifacts == [_answer(good)]
        assert resolution.bindings == (
            None if import_errors else [_binding("extract", rel_path="good.artifact")]
        )

    @pytest.mark.parametrize(
        ("task_id", "import_errors"),
        [
            pytest.param("extract", {}, id="handler-found"),
            pytest.param(
                "load",
                {
                    "etl.py": f"{PROBED}\n- Dag 'etl', task 'load': no artifact in Dag bundle 'task-handlers' "
                    "registers it; no answer from 'bad.artifact' "
                    "(probe failed: OSError: Cannot allocate memory)"
                },
                id="handler-missing",
            ),
        ],
    )
    def test_a_probe_that_raises_is_ignored_and_named_when_a_handler_is_missing(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog, task_id, import_errors
    ):
        configure()
        write_artifact(artifacts / "bad.artifact")
        good = write_artifact(artifacts / "good.artifact", task_handlers=_declare("extract"))

        def probe(**kwargs):
            if kwargs["artifact_rel_path"] == "bad.artifact":
                raise OSError("Cannot allocate memory")
            return _probe(**kwargs)

        mock_run.side_effect = probe

        resolution = _resolve([_stub(task_id)], dag_bundle)

        assert resolution.import_errors == import_errors
        assert resolution.probed_artifacts == [_answer(good)]
        [entry] = [e for e in cap_structlog.entries if e["event"] == "Probing a task handler artifact raised"]
        assert entry["path"] == "bad.artifact"
        assert entry["exception"][0]["exc_type"] == "OSError"

    def test_a_candidate_past_the_deadline_is_not_probed(self, mock_run, configure, dag_bundle, artifacts):
        configure()
        write_artifact(artifacts / "a.artifact", task_handlers=_declare("extract"))
        write_artifact(artifacts / "b.artifact", task_handlers=_declare("extract"))
        deadline = time.monotonic() + 0.5

        def stop_at_the_deadline(**kwargs):
            # As a probe still running at its deadline is stopped.
            time.sleep(max(deadline - time.monotonic(), 0) + 0.05)
            return _stop_probe(**kwargs)

        mock_run.side_effect = stop_at_the_deadline

        resolution = _resolve([_stub("load")], dag_bundle, deadline=deadline)

        assert resolution == TaskHandlerResolution(
            bindings=None,
            probed_artifacts=[],
            import_errors={
                "etl.py": f"{PROBED}\n- Dag 'etl', task 'load': no artifact in Dag bundle 'task-handlers' "
                "registers it; no answer from "
                "'a.artifact' (probe stopped: the parse ran out of [dag_processor] dag_file_processor_timeout), "
                "'b.artifact' (not probed: the parse ran out of [dag_processor] dag_file_processor_timeout)"
            },
        )
        assert [c.kwargs["artifact_rel_path"] for c in mock_run.call_args_list] == ["a.artifact"]

    def test_a_later_coordinators_skip_does_not_hide_an_earlier_coordinators_failure(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        _configure_two_coordinators(configure)
        write_artifact(artifacts / "x.artifact")
        write_artifact(artifacts / "y.artifact")
        deadline = time.monotonic() + 0.5

        def fail_x_and_run_y_to_the_deadline(**kwargs):
            if kwargs["artifact_rel_path"] == "x.artifact":
                return _fail_probe(**kwargs)
            time.sleep(max(deadline - time.monotonic(), 0) + 0.05)
            return _stop_probe(**kwargs)

        mock_run.side_effect = fail_x_and_run_y_to_the_deadline

        resolution = _resolve(
            [_stub("extract"), _stub("extract", queue="other-queue", relative_fileloc="other.py")],
            dag_bundle,
            deadline=deadline,
        )

        no_handler = "no artifact in Dag bundle 'task-handlers' registers it; no answer from"
        out_of_time = "the parse ran out of [dag_processor] dag_file_processor_timeout"
        assert resolution == TaskHandlerResolution(
            bindings=None,
            probed_artifacts=[],
            import_errors={
                "etl.py": f"{PROBED}\n- Dag 'etl', task 'extract': {no_handler} "
                "'x.artifact' (probe failed: The Lang-SDK runtime exited with code 1 without a parse result), "
                f"'y.artifact' (probe stopped: {out_of_time})",
                "other.py": "Stub tasks in other.py do not match their task handlers:\n"
                f"- Dag 'etl', task 'extract': {no_handler} "
                f"'x.artifact' (not probed: {out_of_time}), 'y.artifact' (not probed: {out_of_time})",
            },
        )
        probed = [(c.kwargs["coordinator"], c.kwargs["artifact_rel_path"]) for c in mock_run.call_args_list]
        assert probed == [("fake", "x.artifact"), ("fake", "y.artifact")]

    def test_an_error_the_runtime_reported_is_not_blamed_on_the_deadline(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        configure()
        write_artifact(artifacts / "etl.artifact")
        deadline = time.monotonic() + 0.5

        def answer_with_an_error_and_run_past_the_deadline(**kwargs):
            time.sleep(max(deadline - time.monotonic(), 0) + 0.05)
            return _fail_probe(error="handler registry failed", **kwargs)

        mock_run.side_effect = answer_with_an_error_and_run_past_the_deadline

        resolution = _resolve([_stub("extract")], dag_bundle, deadline=deadline)

        assert resolution.import_errors == {
            "etl.py": f"{PROBED}\n- Dag 'etl', task 'extract': no artifact in Dag bundle 'task-handlers' "
            "registers it; no answer from 'etl.artifact' (probe failed: handler registry failed)"
        }

    def test_a_bundle_of_another_team_is_an_import_error(self, mock_run, configure, dag_bundle, artifacts):
        configure(
            bundles=[
                {
                    "name": "dags",
                    "classpath": LOCAL_BUNDLE,
                    "kwargs": {"path": os.fspath(dag_bundle)},
                    "team_name": "a",
                },
                {
                    "name": "task-handlers",
                    "classpath": LOCAL_BUNDLE,
                    "kwargs": {"path": os.fspath(artifacts)},
                    "team_name": "b",
                },
            ],
            multi_team=True,
        )
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract"))

        resolution = _resolve([_stub("extract")], dag_bundle)

        assert resolution == TaskHandlerResolution(
            bindings=None,
            probed_artifacts=[],
            import_errors={
                "etl.py": f"{PROBED}\n- Coordinator 'fake': Dag bundle 'task-handlers' belongs to team 'b', "
                "but Dag bundle 'dags' belongs to team 'a'"
            },
        )
        mock_run.assert_not_called()

    def test_a_coordinator_without_a_bundle_name_reads_the_dag_bundle(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        # The Dag bundle is not looked up in the configuration: the parse request carries its path.
        configure(
            {"fake": {"classpath": FAKE_COORDINATOR}},
            bundles=[
                {"name": "elsewhere", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(artifacts)}}
            ],
        )
        (dag_bundle / "etl.py").write_text("")
        artifact = write_artifact(dag_bundle / "etl.artifact", task_handlers=_declare("extract"))

        resolution = _resolve([_stub("extract")], dag_bundle)

        assert resolution == TaskHandlerResolution(
            bindings=[_binding("extract", bundle_name="dags")],
            probed_artifacts=[_answer(artifact, bundle_name="dags")],
            import_errors={},
        )
        assert mock_run.call_args.kwargs["bundle_path"] == dag_bundle

    @patch.object(DagBundlesManager, "get_bundle", autospec=True, side_effect=DagBundlesManager.get_bundle)
    @patch.object(DagBundlesManager, "__init__", autospec=True, side_effect=DagBundlesManager.__init__)
    def test_a_bundle_two_coordinators_name_is_looked_up_once(
        self, mock_init, mock_get_bundle, mock_run, configure, dag_bundle, artifacts
    ):
        _configure_two_coordinators(configure)
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", "load"))

        resolution = _resolve([_stub("extract"), _stub("load", queue="other-queue")], dag_bundle)

        assert resolution.import_errors == {}
        assert mock_init.call_count == 1
        assert [c.args[1] for c in mock_get_bundle.call_args_list] == ["task-handlers"]

    def test_a_missing_bundle_path_is_an_import_error(self, mock_run, configure, dag_bundle, artifacts):
        configure()
        artifacts.rmdir()

        resolution = _resolve([_stub("extract")], dag_bundle)

        assert resolution.import_errors == {
            "etl.py": f"{PROBED}\n- Coordinator 'fake': Dag bundle 'task-handlers' resolved to {artifacts}, "
            "which does not exist on this Dag processor"
        }
        assert resolution.bindings is None

    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads every directory")
    @pytest.mark.parametrize("locked", ["parent", "root"])
    def test_an_unreadable_bundle_path_is_an_import_error(
        self, mock_run, configure, dag_bundle, tmp_path, locked
    ):
        parent = tmp_path / "locked"
        root = parent / "artifacts"
        root.mkdir(parents=True)
        configure(
            bundles=[
                {"name": "dags", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(dag_bundle)}},
                {"name": "task-handlers", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(root)}},
            ]
        )
        write_artifact(root / "etl.artifact", task_handlers=_declare("extract"))
        (parent if locked == "parent" else root).chmod(0)
        try:
            resolution = _resolve([_stub("extract")], dag_bundle)
        finally:
            parent.chmod(0o700)
            root.chmod(0o700)

        assert resolution.bindings is None
        assert resolution.import_errors == {
            "etl.py": f"{PROBED}\n- Coordinator 'fake': Dag bundle 'task-handlers' at {root} cannot be read "
            f"on this Dag processor: PermissionError: [Errno 13] Permission denied: '{root}'"
        }
        mock_run.assert_not_called()

    @pytest.mark.parametrize(
        ("listing", "error"),
        [
            pytest.param(
                SubprocessCoordinator.list_task_handler_candidates,
                f"{FAKE_COORDINATOR} cannot list task handler artifacts, so its stub tasks cannot be bound",
                id="not-implemented",
            ),
            pytest.param(
                OSError("permission denied"),
                "cannot list the task handler artifacts of Dag bundle 'task-handlers': "
                "OSError: permission denied",
                id="fails",
            ),
        ],
    )
    def test_a_coordinator_that_cannot_list_is_an_import_error(
        self, mock_run, configure, dag_bundle, listing, error
    ):
        configure()
        if isinstance(listing, Exception):
            patcher = patch.object(
                FakeCoordinator, "list_task_handler_candidates", autospec=True, side_effect=listing
            )
        else:
            patcher = patch.object(
                FakeCoordinator,
                "_read_task_handler_candidate",
                SubprocessCoordinator._read_task_handler_candidate,
            )

        with patcher:
            resolution = _resolve(
                [
                    _stub("extract"),
                    _stub("load", relative_fileloc="other.py"),
                    _stub("report", queue="default"),
                ],
                dag_bundle,
            )

        assert resolution.import_errors == {
            "etl.py": f"{PROBED}\n- Coordinator 'fake': {error}",
            "other.py": f"Stub tasks in other.py do not match their task handlers:\n- Coordinator 'fake': {error}",
        }
        assert resolution.bindings is None

    def test_a_coordinator_that_cannot_be_built_is_an_import_error(self, mock_run, configure, dag_bundle):
        configure(
            {
                "fake": {
                    "classpath": "no_such_module.Coordinator",
                    "kwargs": {"task_handler_bundle_name": "task-handlers"},
                }
            }
        )

        resolution = _resolve([_stub("extract")], dag_bundle)

        assert resolution.import_errors == {
            "etl.py": f"{PROBED}\n- Coordinator 'fake': cannot be built: InvalidCoordinatorError: "
            "Cannot import coordinator 'fake' (ModuleNotFoundError: No module named 'no_such_module')"
        }

    @patch(
        "airflow.dag_processing.task_handler_resolution.plan_task_handler_probes",
        autospec=True,
        side_effect=ValueError("unexpected listing"),
    )
    def test_an_unexpected_error_in_a_coordinator_is_an_import_error(
        self, mock_plan, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract"))

        resolution = _resolve([_stub("extract"), _stub("load", relative_fileloc="other.py")], dag_bundle)

        error = "Coordinator 'fake': cannot find its task handler artifacts: ValueError: unexpected listing"
        assert resolution == TaskHandlerResolution(
            bindings=None,
            probed_artifacts=[],
            import_errors={
                "etl.py": f"{PROBED}\n- {error}",
                "other.py": f"Stub tasks in other.py do not match their task handlers:\n- {error}",
            },
        )
        [entry] = [
            e
            for e in cap_structlog.entries
            if e["event"] == "Cannot list the task handler artifacts of a coordinator"
        ]
        assert entry["coordinator"] == "fake"
        assert entry["exception"][0]["exc_type"] == "ValueError"

    def test_an_invalid_sdk_config_is_an_import_error(self, mock_run, configure, dag_bundle):
        configure(queue_to_coordinator={"fake-queue": "missing"})

        resolution = _resolve([_stub("extract"), _stub("load", relative_fileloc="other.py")], dag_bundle)

        error = (
            "Cannot load [sdk] coordinators: "
            "ValueError: [sdk] queue_to_coordinator references invalid coordinator key: 'missing'"
        )
        assert resolution == TaskHandlerResolution(
            bindings=None,
            probed_artifacts=[],
            import_errors={
                "etl.py": f"{PROBED}\n- {error}",
                "other.py": f"Stub tasks in other.py do not match their task handlers:\n- {error}",
            },
        )

    def test_a_name_mismatch_is_logged_in_the_parse_log(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()
        params = [
            {"name": "region_code", "exact_name": True, "value_schema": {"type": "string"}},
            {"name": "not_in_dag", "exact_name": True},
        ]
        write_artifact(
            artifacts / "etl.artifact",
            task_handlers={"etl": [{"task_id": "extract", "binding": "named", "params": params}]},
        )
        arg_bindings = [
            LiteralArgBinding(kind="literal", name=name, value="eu")
            for name in ("region_code", "unused_label")
        ]

        resolution = _resolve([_stub("extract", arg_bindings=arg_bindings)], dag_bundle)

        assert resolution.bindings == [_binding("extract")]
        assert resolution.import_errors == {}
        context = {
            "dag_id": "etl",
            "task_id": "extract",
            "artifact_bundle_name": "task-handlers",
            "artifact_rel_path": "etl.artifact",
            "log_level": "warning",
        }
        assert {
            "event": "Dag's call passed argument(s) the task handler does not declare",
            "passed_not_declared": ["unused_label"],
            **context,
        } in cap_structlog
        assert {
            "event": "Task handler declares argument(s) the Dag's call did not pass",
            "declared_not_passed": ["not_in_dag"],
            **context,
        } in cap_structlog
