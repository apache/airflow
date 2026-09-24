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
"""E2E test for the Go SDK ``taskflow_binding_dag`` example.

The stub Dag's single mixed positional/keyword TaskFlow call carries literals
of every scalar type, an array literal, a defaulted ``None``, and XComs from
two upstream Go tasks (an object bound onto a strict Go struct and an array
bound onto ``[]int``). The Go ``via_flat_args`` task verifies every bound
value and errors on any mismatch, so a green run *is* the binding assertion;
the tests here check the run outcome and the summary XCom it pushes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# Allow time for coordinator startup.
_GO_TASK_TIMEOUT = 300

# Task logs land shortly after the run finishes.
_LOG_FETCH_TIMEOUT = 60

_DAG_ID = "taskflow_binding_dag"


@dataclass
class _CompletedRun:
    """The single ``taskflow_binding_dag`` run shared across this module's tests."""

    client: AirflowClient
    run_id: str
    state: str
    ti_states: dict[str, str]

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key=key).get(
            "value"
        )

    def logs(self, task_id: str, try_number: int = 1) -> str:
        """Return the concatenated task-log records for *task_id*, retrying until present."""
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        while True:
            resp = self.client.get_task_logs(
                dag_id=_DAG_ID, run_id=self.run_id, task_id=task_id, try_number=try_number
            )
            text = "\n".join(str(entry) for entry in resp.get("content", []) if isinstance(entry, dict))
            if text.strip() or time.monotonic() > deadline:
                return text
            time.sleep(3)


@pytest.fixture(scope="module")
def completed_run() -> _CompletedRun:
    """Trigger ``taskflow_binding_dag`` once and wait for it to finish."""
    client = AirflowClient()
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_GO_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, run_id=run_id, state=state, ti_states=ti_states)


def test_all_tasks_succeeded(completed_run: _CompletedRun):
    """The Go ``via_flat_args`` task errors on any mis-bound argument, so success here
    proves every literal, XCom, keyword, and defaulted-None binding was correct."""
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )
    for task_id in (
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
    ):
        assert completed_run.ti_states.get(task_id) == "success", completed_run.ti_states


def test_upstream_xcoms_keep_their_shapes(completed_run: _CompletedRun):
    """The Go struct arrives as an object XCom, the ``[]int`` as an array, the region as a string."""
    assert completed_run.xcom("make_config") == {
        "environment": "production",
        "region": "eu-west-1",
        "debug": True,
    }
    assert completed_run.xcom("make_numbers") == [1, 1, 2, 3, 5, 8]
    assert completed_run.xcom("make_region") == "eu-west-1"


def test_via_flat_args_summary_reflects_bound_arguments(completed_run: _CompletedRun):
    """``via_flat_args`` re-emits every bound value, confirming types survived the
    Python literal / XCom -> Go parameter -> XCom round trip."""
    assert completed_run.xcom("via_flat_args") == {
        "name": "summary",
        "count": 3,
        "ratio": 2.5,
        "enabled": True,
        "tags": ["metrics", "hourly"],
        "environment": "production",
        "debug": True,
        "sum": 20,
        "note_was_null": True,
    }


def test_via_struct_no_tags_reflects_bound_arguments(completed_run: _CompletedRun):
    """``via_struct_no_tags`` demonstrates the Go SDK's name-based struct binding
    with no field tags at all: each field falls back to its own Go name, matched
    case- and underscore-insensitively, so ``RegionCode`` binds the idiomatic
    ``region_code``. The region is ``make_region``'s XCom, so a struct field
    binds an XCom-sourced value here."""
    assert completed_run.xcom("via_struct_no_tags") == {
        "region_code": "eu-west-1",
        "threshold": 0.75,
    }


def test_via_struct_arg_tag_reflects_bound_arguments(completed_run: _CompletedRun):
    """``via_struct_arg_tag`` demonstrates explicit ``arg:`` tags: ``Region`` is
    genuinely renamed to ``region_code`` (bound from ``make_region``'s XCom), and
    ``Threshold`` is tagged ``threshold`` to pull the snake_case literal its
    verbatim field name would miss."""
    assert completed_run.xcom("via_struct_arg_tag") == {
        "region": "eu-west-1",
        "threshold": 0.75,
    }


def test_via_struct_default_arg_tolerates_unclaimed_default(completed_run: _CompletedRun):
    """``via_struct_default_arg`` proves a captured stub default needs no struct field:
    ``sample_rate`` is unpassed, so the spec carries it as ``from_default`` and no Go
    field claims it. The task succeeding at all is the assertion."""
    assert completed_run.xcom("via_struct_default_arg") == {"region": "eu-west-1"}


def test_via_struct_more_args_warns_and_runs(completed_run: _CompletedRun):
    """The call passes ``unused_label``, which the Go struct does not declare. Name
    binding cannot shift, so the extra argument is warned about rather than failing
    the task, and everything the struct does declare still binds."""
    assert completed_run.xcom("via_struct_more_args") == {"region": "eu-west-1"}
    logs = completed_run.logs("via_struct_more_args")
    assert "Dag's call passed argument(s) the task handler does not declare" in logs, logs
    assert "unused_label" in logs, logs


def test_via_struct_fewer_args_warns_and_runs(completed_run: _CompletedRun):
    """The Go struct declares ``not_in_dag``, which the stub has no parameter for. The
    field keeps its Go zero value and the mismatch is warned about rather than failing
    the task, so the two sides can drift without breaking the Dag.

    The warning is the assertion that matters: the zero-valued field alone would look
    the same as the older behaviour that filled it silently."""
    assert completed_run.xcom("via_struct_fewer_args") == {
        "region": "eu-west-1",
        "not_in_dag_was_empty": True,
    }
    logs = completed_run.logs("via_struct_fewer_args")
    assert "Task handler declares argument(s) the Dag's call did not pass" in logs, logs
    assert "not_in_dag" in logs, logs


def test_via_flat_map_decodes_single_dict_whole(completed_run: _CompletedRun):
    """``via_flat_map`` passes one dict literal whose argument name matches no Go
    struct field, so the whole map is decoded into the struct (flat binding)."""
    assert completed_run.xcom("via_flat_map") == {"region": "eu-west-1", "count": 3}


def test_via_struct_map_binds_single_dict_onto_map_field(completed_run: _CompletedRun):
    """``via_struct_map`` passes one dict literal whose argument name binds by name
    onto a Go struct's ``map`` field (struct-based binding)."""
    assert completed_run.xcom("via_struct_map") == {
        "payload": {"region": "eu-west-1", "count": 3},
    }


def test_via_plain_map_decodes_dict_into_a_typed_map(completed_run: _CompletedRun):
    """``via_plain_map`` passes one dict literal onto a Go ``map[string]string``
    parameter, so it decodes with no struct involved at all."""
    assert completed_run.xcom("via_plain_map") == {"team": "data", "tier": "gold"}
