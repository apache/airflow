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
import os
import shutil
import time
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import structlog

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import LiteralArgBinding
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.processor import TaskHandlerParseRequest, TaskHandlerParsingResult
from airflow.dag_processing.task_handler_processor import LangSDKTaskHandlerProcessorProcess
from airflow.dag_processing.task_handler_resolution import resolve_task_handlers
from airflow.dag_processing.task_handler_validation import StubTask
from airflow.sdk.execution_time.coordinator import BaseCoordinator

from tests_common.test_utils.config import conf_vars
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

PROBLEMS = "Stub tasks in etl.py do not match their task handlers:"
INTEGER = {"type": "integer"}
STRING = {"type": "string"}


def _stub(
    task_id: str,
    *,
    dag_id: str = "etl",
    queue: str = "fake-queue",
    relative_fileloc: str | None = None,
    arg_bindings=(),
    is_mapped: bool = False,
) -> StubTask:
    return StubTask(
        dag_id=dag_id,
        task_id=task_id,
        queue=queue,
        relative_fileloc=relative_fileloc or f"{dag_id}.py",
        arg_bindings=list(arg_bindings),
        is_mapped=is_mapped,
    )


def _literal(name: str, value, *, value_schema=None) -> LiteralArgBinding:
    return LiteralArgBinding(name=name, kind="literal", value=value, value_schema=value_schema)


def _declare(*task_ids: str, dag_id: str = "etl", binding: str = "positional", params=None) -> dict:
    return {
        dag_id: [
            {"task_id": task_id, "binding": binding, "params": [] if params is None else params}
            for task_id in task_ids
        ]
    }


def _probe(*, path, bundle_path, bundle_name, **kwargs) -> TaskHandlerParsingResult:
    """Answer as the runtime does, from the ``task_handlers`` of the artifact's JSON."""
    request = TaskHandlerParseRequest(file=os.fspath(path), bundle_path=bundle_path, bundle_name=bundle_name)
    return reply_with_task_handlers(request, None)


def _fail_probe(
    *,
    path,
    artifact_rel_path,
    error="The Lang-SDK runtime exited with code 1 without a parse result",
    **kwargs,
) -> TaskHandlerParsingResult:
    return TaskHandlerParsingResult(
        fileloc=os.fspath(path), task_handlers={}, import_errors={artifact_rel_path: error}
    )


def _probe_raises(**kwargs) -> TaskHandlerParsingResult:
    raise RuntimeError("boom")


def _probe_fails(**kwargs) -> TaskHandlerParsingResult:
    return _fail_probe(path=kwargs["path"], artifact_rel_path=kwargs["artifact_rel_path"])


def _probe_times_out(**kwargs) -> TaskHandlerParsingResult:
    return _fail_probe(
        path=kwargs["path"],
        artifact_rel_path=kwargs["artifact_rel_path"],
        error=f"The Lang-SDK runtime did not parse {kwargs['path']} by its deadline",
    )


class _PlainCoordinator(BaseCoordinator):
    """A coordinator of no particular class, so it cannot probe task handlers."""


class _ExplodingCoordinator(BaseCoordinator):
    def __init__(self, **kwargs):
        raise RuntimeError("cannot build this coordinator")


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


def _resolve(stub_tasks, dag_bundle, *, deadline: float | None = None) -> dict[str, str]:
    return resolve_task_handlers(
        stub_tasks,
        dag_bundle_name="dags",
        dag_bundle_path=dag_bundle,
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


def _not_checked_entries(cap_structlog) -> list[dict]:
    return [
        e
        for e in cap_structlog.entries
        if e["event"] == "Not checking a Dag's stub tasks against their task handlers"
    ]


@mock.patch.object(LangSDKTaskHandlerProcessorProcess, "run", autospec=True, side_effect=_probe)
class TestResolveTaskHandlers:
    def test_every_routed_stub_task_is_checked_against_the_artifact_of_its_dag(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", dag_id="etl"))
        write_artifact(
            artifacts / "reporting.artifact", task_handlers=_declare("summarize", dag_id="reporting")
        )
        deadline = time.monotonic() + 60

        import_errors = _resolve(
            [_stub("extract", dag_id="etl"), _stub("summarize", dag_id="reporting")],
            dag_bundle,
            deadline=deadline,
        )

        assert import_errors == {}
        assert mock_run.call_count == 2
        for call in mock_run.call_args_list:
            assert call.kwargs["coordinator"] == "fake"
            assert call.kwargs["bundle_path"] == artifacts
            assert call.kwargs["bundle_name"] == "task-handlers"
            assert call.kwargs["deadline"] == deadline
        assert sorted(call.kwargs["artifact_rel_path"] for call in mock_run.call_args_list) == [
            "etl.artifact",
            "reporting.artifact",
        ]
        assert {"event": "Probing a task handler artifact", "path": "etl.artifact"} in cap_structlog
        assert {"event": "Probed a task handler artifact", "path": "etl.artifact"} in cap_structlog
        assert {"event": "Probing a task handler artifact", "path": "reporting.artifact"} in cap_structlog
        assert {"event": "Probed a task handler artifact", "path": "reporting.artifact"} in cap_structlog

    def test_a_stub_task_on_an_unrouted_queue_is_not_checked(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract"))

        import_errors = _resolve([_stub("extract"), _stub("other", queue="unrouted")], dag_bundle)

        assert import_errors == {}
        assert mock_run.call_count == 1

    def test_one_artifact_serving_two_dags_is_probed_once(self, mock_run, configure, dag_bundle, artifacts):
        configure()
        write_artifact(
            artifacts / "shared.artifact",
            task_handlers={
                "etl": [{"task_id": "extract", "binding": "positional", "params": []}],
                "reporting": [{"task_id": "summarize", "binding": "positional", "params": []}],
            },
        )

        import_errors = _resolve(
            [_stub("extract", dag_id="etl"), _stub("summarize", dag_id="reporting")], dag_bundle
        )

        assert import_errors == {}
        assert mock_run.call_count == 1

    def test_the_probed_artifact_is_the_one_the_find_hook_returns(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", dag_id="etl"))
        write_artifact(artifacts / "decoy.artifact", task_handlers={})

        import_errors = _resolve([_stub("extract", dag_id="etl")], dag_bundle)

        assert import_errors == {}
        assert mock_run.call_count == 1
        assert mock_run.call_args.kwargs["artifact_rel_path"] == "etl.artifact"

    def test_a_missing_task_handler_is_an_import_error(self, mock_run, configure, dag_bundle, artifacts):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers={})

        import_errors = _resolve([_stub("load")], dag_bundle)

        assert import_errors == {
            "etl.py": f"{PROBLEMS}\n"
            "- Dag 'etl', task 'load': 'etl.artifact' in Dag bundle 'task-handlers' registers no task handler for it"
        }

    @pytest.mark.parametrize(
        ("arg_bindings", "params", "expected_detail"),
        [
            pytest.param(
                [_literal("count", 1)],
                [{"name": "count", "value_schema": INTEGER}, {"name": "label", "value_schema": STRING}],
                "passes 1 argument, the task handler takes 2",
                id="positional-count",
            ),
            pytest.param(
                [_literal("count", "nope", value_schema=STRING)],
                [{"name": "count", "value_schema": INTEGER}],
                "argument 'count' is string, the task handler takes integer",
                id="value-type",
            ),
        ],
    )
    def test_an_argument_mismatch_is_an_import_error(
        self, mock_run, configure, dag_bundle, artifacts, arg_bindings, params, expected_detail
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("load", params=params))

        import_errors = _resolve([_stub("load", arg_bindings=arg_bindings)], dag_bundle)

        assert import_errors == {
            "etl.py": f"{PROBLEMS}\n"
            f"- Dag 'etl', task 'load' ('etl.artifact' in Dag bundle 'task-handlers'): {expected_detail}"
        }

    def test_a_name_mismatch_is_logged_not_an_import_error(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()
        params = [{"name": "region", "value_schema": STRING}]
        write_artifact(
            artifacts / "etl.artifact", task_handlers=_declare("load", binding="named", params=params)
        )

        import_errors = _resolve(
            [
                _stub(
                    "load",
                    arg_bindings=[
                        _literal("region", "us", value_schema=STRING),
                        _literal("extra", 1, value_schema=INTEGER),
                    ],
                )
            ],
            dag_bundle,
        )

        assert import_errors == {}
        assert {
            "event": "Dag's call passed argument(s) the task handler does not declare",
            "dag_id": "etl",
            "task_id": "load",
            "artifact_bundle_name": "task-handlers",
            "artifact_rel_path": "etl.artifact",
            "passed_not_declared": ["extra"],
        } in cap_structlog

    @pytest.mark.parametrize(
        "coordinators",
        [
            pytest.param({"fake": {"classpath": "nonexistent.module.NoSuchClass"}}, id="bad-classpath"),
            pytest.param(
                {"fake": {"classpath": f"{__name__}._ExplodingCoordinator"}}, id="constructor-raises"
            ),
        ],
    )
    def test_a_coordinator_that_cannot_be_built_is_a_warning(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog, coordinators
    ):
        configure(coordinators)

        import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        mock_run.assert_not_called()
        [entry] = _not_checked_entries(cap_structlog)
        assert entry["coordinator"] == "fake"
        assert entry["dag_id"] == "etl"
        assert entry["log_level"] == "warning"
        assert "cannot be built" in entry["reason"]

    def test_an_sdk_config_that_cannot_load_is_a_warning(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()

        with conf_vars({("sdk", "queue_to_coordinator"): "{"}):
            import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        mock_run.assert_not_called()
        assert any(
            e["event"] == "Cannot load [sdk] coordinators, so no stub task is checked"
            for e in cap_structlog.entries
        )

    def test_a_coordinators_unconfigured_task_handler_bundle_is_a_warning(
        self, mock_run, dag_bundle, artifacts, cap_structlog
    ):
        bundles = [{"name": "dags", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(dag_bundle)}}]
        coordinators = {
            "fake": {"classpath": FAKE_COORDINATOR, "kwargs": {"task_handler_bundle_name": "missing-bundle"}}
        }

        with task_handler_config(dag_bundle, artifacts, coordinators, bundles=bundles):
            import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        mock_run.assert_not_called()
        assert any(
            e["event"] == "Cannot load [sdk] coordinators, so no stub task is checked"
            for e in cap_structlog.entries
        )

    @pytest.mark.parametrize(
        "classpath",
        [
            pytest.param(f"{__name__}._PlainCoordinator", id="not-a-subprocess-coordinator"),
            pytest.param("airflow.sdk.coordinators._subprocess.SubprocessCoordinator", id="no-find-hook"),
        ],
    )
    def test_a_coordinator_that_does_not_probe_is_skipped_at_debug(
        self, mock_run, dag_bundle, artifacts, cap_structlog, classpath
    ):
        cap_structlog.set_level("debug")
        coordinators = {"fake": {"classpath": classpath}}

        with task_handler_config(dag_bundle, artifacts, coordinators):
            import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        mock_run.assert_not_called()
        [entry] = _not_checked_entries(cap_structlog)
        assert entry["log_level"] == "debug"

    @pytest.mark.parametrize(
        "find_error",
        [
            pytest.param(None, id="missing"),
            pytest.param({"type": "PermissionError", "message": "cannot read artifact"}, id="rejected"),
        ],
    )
    def test_no_artifact_for_the_dag_is_a_warning(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog, find_error
    ):
        configure()
        if find_error is not None:
            write_artifact(artifacts / "etl.artifact", find_error=find_error)

        import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        mock_run.assert_not_called()
        [entry] = _not_checked_entries(cap_structlog)
        assert entry["log_level"] == "warning"
        assert "no task handler artifact" in entry["reason"]

    def test_an_artifact_bundle_of_another_team_is_a_warning(
        self, mock_run, dag_bundle, artifacts, cap_structlog
    ):
        bundles = [
            {"name": "dags", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(dag_bundle)}},
            {
                "name": "task-handlers",
                "classpath": LOCAL_BUNDLE,
                "kwargs": {"path": os.fspath(artifacts)},
                "team_name": "team-a",
            },
        ]
        coordinators = {
            "fake": {"classpath": FAKE_COORDINATOR, "kwargs": {"task_handler_bundle_name": "task-handlers"}}
        }

        with task_handler_config(dag_bundle, artifacts, coordinators, bundles=bundles, multi_team=True):
            import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        mock_run.assert_not_called()
        [entry] = _not_checked_entries(cap_structlog)
        assert "belongs to team 'team-a'" in entry["reason"]
        assert "belongs to no team" in entry["reason"]

    def test_an_empty_team_name_is_no_team(self, mock_run, dag_bundle, artifacts):
        bundles = [
            {"name": "dags", "classpath": LOCAL_BUNDLE, "kwargs": {"path": os.fspath(dag_bundle)}},
            {
                "name": "task-handlers",
                "classpath": LOCAL_BUNDLE,
                "kwargs": {"path": os.fspath(artifacts)},
                "team_name": "",
            },
        ]
        coordinators = {
            "fake": {"classpath": FAKE_COORDINATOR, "kwargs": {"task_handler_bundle_name": "task-handlers"}}
        }
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract"))

        with task_handler_config(dag_bundle, artifacts, coordinators, bundles=bundles, multi_team=True):
            import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        assert mock_run.call_count == 1

    @pytest.mark.parametrize("mode", ["missing", "unreadable"])
    def test_an_artifact_bundle_that_cannot_be_read_is_a_warning(
        self, mock_run, dag_bundle, artifacts, cap_structlog, mode
    ):
        if mode == "unreadable" and hasattr(os, "geteuid") and os.geteuid() == 0:
            pytest.skip("root bypasses permission checks")
        coordinators = {
            "fake": {"classpath": FAKE_COORDINATOR, "kwargs": {"task_handler_bundle_name": "task-handlers"}}
        }

        with task_handler_config(dag_bundle, artifacts, coordinators):
            if mode == "missing":
                shutil.rmtree(artifacts)
            else:
                artifacts.chmod(0o000)
            try:
                import_errors = _resolve([_stub("extract")], dag_bundle)
            finally:
                if mode == "unreadable":
                    artifacts.chmod(0o755)

        assert import_errors == {}
        mock_run.assert_not_called()
        [entry] = _not_checked_entries(cap_structlog)
        if mode == "missing":
            assert "does not exist on this Dag processor" in entry["reason"]
        else:
            assert "cannot be read on this Dag processor" in entry["reason"]

    def test_an_artifact_whose_sdk_is_too_old_is_not_probed(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract"))

        with mock.patch(
            "airflow.sdk.coordinators._subprocess.TASK_HANDLER_PARSING_SCHEMA_VERSION", "2099-01-01"
        ):
            import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        mock_run.assert_not_called()
        [entry] = _not_checked_entries(cap_structlog)
        assert entry["log_level"] == "info"
        assert "cannot answer a task handler parse request" in entry["reason"]

    @pytest.mark.parametrize(
        "side_effect",
        [
            pytest.param(_probe_raises, id="raises"),
            pytest.param(_probe_fails, id="runtime-failure"),
            pytest.param(_probe_times_out, id="deadline"),
        ],
    )
    def test_a_probe_that_fails_is_a_warning(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog, side_effect
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers={})
        mock_run.side_effect = side_effect

        import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        [entry] = _not_checked_entries(cap_structlog)
        assert entry["log_level"] == "warning"

    def test_an_artifact_past_the_deadline_is_not_probed(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract"))

        import_errors = _resolve([_stub("extract")], dag_bundle, deadline=time.monotonic() - 1)

        assert import_errors == {}
        mock_run.assert_not_called()
        [entry] = _not_checked_entries(cap_structlog)
        assert "dag_file_processor_timeout" in entry["reason"]

    def test_a_deadline_already_past_skips_bundle_resolution_and_the_artifact_scan(
        self, mock_run, configure, dag_bundle, artifacts, cap_structlog
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", dag_id="etl"))
        write_artifact(
            artifacts / "reporting.artifact", task_handlers=_declare("summarize", dag_id="reporting")
        )

        with (
            mock.patch.object(FakeCoordinator, "_find_task_handler_artifact", autospec=True) as mock_find,
            mock.patch.object(DagBundlesManager, "get_bundle", autospec=True) as mock_get_bundle,
        ):
            import_errors = _resolve(
                [_stub("extract", dag_id="etl"), _stub("summarize", dag_id="reporting")],
                dag_bundle,
                deadline=time.monotonic() - 1,
            )

        assert import_errors == {}
        mock_run.assert_not_called()
        mock_find.assert_not_called()
        mock_get_bundle.assert_not_called()
        entries = _not_checked_entries(cap_structlog)
        assert len(entries) == 2
        assert all("dag_file_processor_timeout" in e["reason"] for e in entries)

    def test_the_deadline_stops_only_the_artifacts_it_catches(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", dag_id="etl"))
        write_artifact(
            artifacts / "reporting.artifact", task_handlers=_declare("summarize", dag_id="reporting")
        )
        clock = [1000.0]
        deadline = 1050.0

        def _advance_and_reply(**kwargs):
            clock[0] = 1060.0
            return _probe(**kwargs)

        mock_run.side_effect = _advance_and_reply
        with mock.patch(
            "airflow.dag_processing.task_handler_resolution.time.monotonic", side_effect=lambda: clock[0]
        ):
            import_errors = _resolve(
                [_stub("extract", dag_id="etl"), _stub("summarize", dag_id="reporting")],
                dag_bundle,
                deadline=deadline,
            )

        assert import_errors == {}
        assert mock_run.call_count == 1
        assert mock_run.call_args.kwargs["artifact_rel_path"] == "etl.artifact"

    def test_a_runtime_warning_is_logged(self, mock_run, configure, dag_bundle, artifacts, cap_structlog):
        configure()
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract"))

        def _probe_with_warning(**kwargs):
            return _probe(**kwargs).model_copy(update={"warnings": ["low disk space"]})

        mock_run.side_effect = _probe_with_warning

        import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        assert {
            "event": "The task handler runtime reported a warning",
            "warning": "low disk space",
            "coordinator": "fake",
            "bundle_name": "task-handlers",
            "path": "etl.artifact",
        } in cap_structlog

    def test_an_artifact_outside_its_dag_bundle_is_a_warning(
        self, mock_run, configure, dag_bundle, artifacts, tmp_path, cap_structlog
    ):
        configure()
        outside = tmp_path / "outside"
        outside.mkdir()
        real = write_artifact(outside / "etl.artifact", task_handlers=_declare("extract"))
        (artifacts / "etl.artifact").symlink_to(real)

        import_errors = _resolve([_stub("extract")], dag_bundle)

        assert import_errors == {}
        mock_run.assert_not_called()
        [entry] = _not_checked_entries(cap_structlog)
        assert "is not inside Dag bundle" in entry["reason"]

    def test_problems_of_two_dag_files_are_separate_import_errors(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        configure()
        write_artifact(artifacts / "shared.artifact", task_handlers={})

        import_errors = _resolve(
            [
                _stub("extract", dag_id="a", relative_fileloc="a.py"),
                _stub("load", dag_id="b", relative_fileloc="b.py"),
            ],
            dag_bundle,
        )

        assert set(import_errors) == {"a.py", "b.py"}

    def test_an_artifact_two_coordinators_pick_is_probed_once_per_coordinator(
        self, mock_run, configure, dag_bundle, artifacts
    ):
        _configure_two_coordinators(configure)
        write_artifact(artifacts / "etl.artifact", task_handlers=_declare("extract", "sync"))

        import_errors = _resolve([_stub("extract"), _stub("sync", queue="other-queue")], dag_bundle)

        assert import_errors == {}
        assert mock_run.call_count == 2
        assert {call.kwargs["coordinator"] for call in mock_run.call_args_list} == {"fake", "other"}
