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
E2E tests for what the Dag processor does with the Go stub tasks when it parses a Dag file.

Run with::

    E2E_TEST_MODE=go_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/go_sdk_tests/test_go_sdk_task_handler_parse.py -xvs

The Dag processor probes each Go bundle for the task handlers it registers and checks the Dag file's stub
tasks against them at parse time, against the artifact the worker's own pick would run. These tests read
what it recorded in its parse logs and the REST API. Nothing here triggers or runs a task.
"""

from __future__ import annotations

import json

import pytest
import requests

from airflow_e2e_tests.constants import (
    DAGS_BUNDLE_NAME,
    GO_SDK_BUNDLE_NAME,
    GO_SDK_QUEUE,
    GO_SDK_TASK_HANDLER_BUNDLE,
    GO_TEST_BUNDLE_ARTIFACT,
    GO_TEST_TASK_HANDLER_BUNDLE,
)
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.lang_sdk import (
    PROBED_EVENT,
    PROBING_EVENT,
    ArtifactRef,
    assert_every_file_is_probed_once,
    assert_later_parses_probe_nothing_new,
    get_import_errors,
    get_routed_stub_tasks,
    read_parse_attempts,
    wait_until_stub_tasks_are_checked,
)

_EXAMPLE_ARTIFACT = ArtifactRef(GO_SDK_TASK_HANDLER_BUNDLE, GO_SDK_BUNDLE_NAME)
_TEST_ARTIFACT = ArtifactRef(GO_TEST_TASK_HANDLER_BUNDLE, GO_TEST_BUNDLE_ARTIFACT)

_FAILURE_FILE = "go_task_handler_failures.py"
# The import error of the Dag file whose stub tasks do not match their task handlers, one line for each,
# sorted by task id: "not_registered" sorts before "takes_two_numbers".
_FAILURE_IMPORT_ERROR = "\n".join(
    [
        f"Stub tasks in {_FAILURE_FILE} do not match their task handlers:",
        f"- Dag 'go_task_handler_failures', task 'not_registered': {GO_TEST_BUNDLE_ARTIFACT!r} in Dag bundle "
        f"{GO_TEST_TASK_HANDLER_BUNDLE!r} registers no task handler for it",
        f"- Dag 'go_task_handler_failures', task 'takes_two_numbers' ({GO_TEST_BUNDLE_ARTIFACT!r} in Dag "
        f"bundle {GO_TEST_TASK_HANDLER_BUNDLE!r}): passes 3 arguments, the task handler takes 2",
    ]
)

# Every stub task of taskflow_binding_dag (go-sdk/dags/go_examples.py), one per TaskFlow binding shape.
_TASKFLOW_BINDING_TASKS = {
    "make_config",
    "make_numbers",
    "make_region",
    "via_flat_args",
    "via_struct_no_tags",
    "via_struct_arg_tag",
    "via_struct_default_arg",
    "via_struct_more_args",
    "via_struct_fewer_args",
    "via_flat_map",
    "via_struct_map",
    "via_plain_map",
}


@pytest.fixture(scope="module")
def client() -> AirflowClient:
    return AirflowClient()


def test_every_lang_sdk_dag_file_is_probed_once(airflow_logs_path):
    """
    Every stub Dag file has a probe record, and each artifact is probed at most once per file.

    The example's four Dags all share one artifact, so the per-parse probe cache means the file is probed
    exactly once despite declaring many stub tasks.
    """
    assert_every_file_is_probed_once(
        airflow_logs_path,
        {
            "go_examples.py": {_EXAMPLE_ARTIFACT},
            _FAILURE_FILE: {_TEST_ARTIFACT},
        },
    )


def test_problems_are_one_import_error_of_their_dag_file(client: AirflowClient, lang_sdk_dag_files):
    """
    A missing handler and a positional count mismatch are one import error, one line each.

    The error is the one of the file, and lists each problem on a line of its own. Its Dag is serialized but
    marked as having import errors.
    """
    assert get_import_errors(client, [_FAILURE_FILE]).get(_FAILURE_FILE) == _FAILURE_IMPORT_ERROR

    # Scoped to this mode's own Dag files: the Dags folder also holds stock example Dags (such as
    # example_event_driven.py) that may fail to import for reasons that have nothing to do with this check.
    failing_dags = client.list_dags(bundle_name=DAGS_BUNDLE_NAME, exclude_stale=False, has_import_errors=True)
    failing_lang_sdk_dags = [
        dag["dag_id"] for dag in failing_dags if dag["relative_fileloc"] in lang_sdk_dag_files
    ]
    assert failing_lang_sdk_dags == ["go_task_handler_failures"]


def test_named_mismatches_are_warnings_in_the_parse_log(airflow_logs_path):
    """
    A Dag call that passes an argument the handler does not declare, and the other way round, only warn.

    ``taskflow_binding_dag`` binds its arguments by name, so the Dag file imports and the Dag processor logs
    each mismatch when it parses ``go_examples.py``.
    """
    records = [
        record for attempt in read_parse_attempts(airflow_logs_path, "go_examples.py") for record in attempt
    ]
    expected = [
        {
            "event": "Dag's call passed argument(s) the task handler does not declare",
            "task_id": "via_struct_more_args",
            "passed_not_declared": ["unused_label"],
        },
        {
            "event": "Task handler declares argument(s) the Dag's call did not pass",
            "task_id": "via_struct_fewer_args",
            "declared_not_passed": ["not_in_dag"],
        },
    ]
    for warning in expected:
        context = {
            **warning,
            "level": "warning",
            "dag_id": "taskflow_binding_dag",
            "artifact_bundle_name": GO_SDK_TASK_HANDLER_BUNDLE,
            "artifact_rel_path": GO_SDK_BUNDLE_NAME,
        }
        assert any(context.items() <= record.items() for record in records), (
            f"No parse of go_examples.py logged {context}. Records of its parses: "
            f"{[record for record in records if record.get('level') == 'warning']}"
        )


def test_a_later_parse_probes_nothing_new(client: AirflowClient, airflow_logs_path):
    """A file parsed again probes no artifact its first parse did not, and none twice."""
    assert_later_parses_probe_nothing_new(
        client,
        airflow_logs_path,
        {"go_examples.py": "simple_dag", _FAILURE_FILE: "go_task_handler_failures"},
    )


def test_only_the_failing_dag_file_has_an_import_error(client: AirflowClient, lang_sdk_dag_files):
    """No other Lang-SDK Dag file fails to import, so no fixture hides behind the expected error."""
    assert set(get_import_errors(client, lang_sdk_dag_files)) == {_FAILURE_FILE}


def test_the_examples_stub_tasks_are_serialized(client: AirflowClient):
    """
    The example's stub tasks serialize and run on the queue the Dag processor routes.

    Its file has no import error (unlike the failure fixture's), so ``extract`` and every TaskFlow
    binding-shape task of ``taskflow_binding_dag`` are routed stub tasks: the stub tasks still run the
    artifact the worker picks, main's existing behavior.
    """
    routed = get_routed_stub_tasks(client, [GO_SDK_QUEUE])
    expected_tasks = {
        ("simple_dag", "extract"),
        *(("taskflow_binding_dag", task) for task in _TASKFLOW_BINDING_TASKS),
    }
    assert expected_tasks <= set(routed), (
        f"Stub tasks the Dag processor did not serialize: {expected_tasks - set(routed)}"
    )


# The tests below exercise the pure parse-log and REST-response logic of ``lang_sdk.py`` and ``clients.py``
# against synthetic logs and a fake client, not the real stack: they live here, rather than next to that
# logic, so they run as part of an existing e2e CI job (this module's). Any Lang-SDK mode's job would do;
# nothing below is Go-specific.


def _log_file(logs_path, relative_fileloc, date="2026-01-01"):
    log_dir = logs_path / "dag_processor" / date / DAGS_BUNDLE_NAME
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"{relative_fileloc}.log"


def _append_attempt(log_file, events):
    """Append one parse attempt (a "Filling up the DagBag" record, then *events*) to *log_file*."""
    lines = [json.dumps({"event": "Filling up the DagBag from X"})]
    lines.extend(json.dumps(event) for event in events)
    with log_file.open("a") as f:
        f.write("\n".join(lines) + "\n")


class _FakeClient:
    """A minimal stand-in for AirflowClient, backed by an in-memory Dag and import error list."""

    def __init__(self, dags, import_errors=()):
        self.dags = dags
        self.import_errors = list(import_errors)
        self.reparse_side_effect = None

    def list_dags(self, bundle_name, exclude_stale=False, **params):
        return self.dags

    def list_import_errors(self, bundle_name, **params):
        return self.import_errors

    def reparse_dag_file(self, file_token):
        if self.reparse_side_effect is not None:
            self.reparse_side_effect()


def test_gate_timeout_distinguishes_probe_never_started_from_started_but_incomplete(tmp_path):
    """
    The gate's timeout message separates a probe that never started from one that started but failed.

    The first points at the artifact bundle missing on the Dag processor; the second, only a warning in the
    parse log, at the artifact itself.
    """
    never_started = "never_started.py"
    started_no_result = "started_no_result.py"
    _append_attempt(_log_file(tmp_path, never_started), [])
    _append_attempt(
        _log_file(tmp_path, started_no_result),
        [{"event": PROBING_EVENT, "bundle_name": "b", "path": "handlers"}],
    )
    client = _FakeClient(
        dags=[
            {"dag_id": "d1", "relative_fileloc": never_started, "last_parsed_time": "t0", "file_token": "t1"},
            {
                "dag_id": "d2",
                "relative_fileloc": started_no_result,
                "last_parsed_time": "t0",
                "file_token": "t2",
            },
        ]
    )

    with pytest.raises(TimeoutError) as excinfo:
        wait_until_stub_tasks_are_checked(
            client,
            tmp_path,
            dag_files=[never_started, started_no_result],
            expected_import_errors=(),
            timeout=0,
        )

    message = str(excinfo.value)
    assert f"probe never started: ['{never_started}']" in message
    assert f"probe started but produced no result: ['{started_no_result}']" in message


def test_list_all_does_not_truncate_when_the_server_clamps_the_page_size(monkeypatch):
    """A server whose ``[api] maximum_page_limit`` clamps a page below what was asked must not lose rows."""
    total_entries = 150
    server_page_size = 60  # below the 100 _list_all requests
    all_items = [{"id": i} for i in range(total_entries)]

    def fake_make_request(method, endpoint, params=None, **kwargs):
        offset = params["offset"]
        return {"things": all_items[offset : offset + server_page_size], "total_entries": total_entries}

    fake_client = AirflowClient.__new__(AirflowClient)
    monkeypatch.setattr(fake_client, "_make_request", fake_make_request)

    assert fake_client._list_all("things", "things") == all_items


def test_a_pending_reparse_request_does_not_fail_the_later_parse_check(tmp_path):
    """A reparse request that 500s because one is already pending must not fail the check outright."""
    file = "go_examples.py"
    dag_id = "simple_dag"
    log_file = _log_file(tmp_path, file)
    _append_attempt(log_file, [{"event": PROBED_EVENT, "bundle_name": "b", "path": "handlers"}])
    client = _FakeClient(
        dags=[{"dag_id": dag_id, "relative_fileloc": file, "last_parsed_time": "t0", "file_token": "tok"}]
    )

    def reparse_conflicts():
        raise requests.HTTPError("409: a parse request for this file is already pending")

    client.reparse_side_effect = reparse_conflicts

    # The parse marker never changes in this fake, so the wait always times out; what matters is that it
    # times out (TimeoutError), not that the HTTPError from the reparse call propagates instead.
    with pytest.raises(TimeoutError):
        assert_later_parses_probe_nothing_new(client, tmp_path, {file: dag_id}, timeout=0.05)


def test_a_later_parse_that_probes_nothing_is_a_failure(tmp_path):
    """
    A later parse of a file must probe something, not pass as a trivial subset of the first parse's probes.

    That is what a probe cache outliving its parse would look like: a regression the subset check alone
    does not catch.
    """
    file = "go_examples.py"
    dag_id = "simple_dag"
    log_file = _log_file(tmp_path, file)
    _append_attempt(log_file, [{"event": PROBED_EVENT, "bundle_name": "b", "path": "handlers"}])
    client = _FakeClient(
        dags=[{"dag_id": dag_id, "relative_fileloc": file, "last_parsed_time": "t0", "file_token": "tok"}]
    )

    def reparse_probes_nothing_new():
        # Simulate the Dag processor parsing the file again (the marker moves, a new attempt is logged)
        # but the probe cache answering from the earlier parse, so no new Probed record is written.
        client.dags[0]["last_parsed_time"] = "t1"
        _append_attempt(log_file, [])

    client.reparse_side_effect = reparse_probes_nothing_new

    with pytest.raises(AssertionError, match="probed nothing"):
        assert_later_parses_probe_nothing_new(client, tmp_path, {file: dag_id}, timeout=5)
