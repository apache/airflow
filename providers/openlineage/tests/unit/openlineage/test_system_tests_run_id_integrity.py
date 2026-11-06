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
"""Tests for ``check_run_id_integrity``, the runId check of the OpenLineage system tests' checking operator."""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from system.openlineage.operator import OpenLineageTestOperator, check_run_id_integrity


def _event(
    run_id: str | None,
    event_type: str = "START",
    *,
    job: str = "dag.task",
    job_type: str = "TASK",
    dag_run_id: str = "run_1",
    clear_number: int = 0,
    try_number: int = 1,
    map_index: int | None = None,
    loop_index: int | None = None,
    region_id: str | None = None,
    loop_group: str | None = None,
    loop_region_id: str | None = None,
    trajectory: int | None = None,
    ti_id: str | None = None,
) -> dict:
    run: dict = {"facets": {}}
    if run_id is not None:
        run["runId"] = run_id
    dag_run = {"run_id": dag_run_id, "clear_number": clear_number}
    if job_type == "TASK":
        task_instance: dict[str, Any] = {"try_number": try_number}
        if map_index is not None:
            task_instance["map_index"] = map_index
        loop = {"index": loop_index, "region_id": loop_region_id, "trajectory": trajectory}
        if loop := {name: value for name, value in loop.items() if value is not None}:
            task_instance["loop"] = loop
        if region_id is not None:
            task_instance["region_id"] = region_id
        if ti_id is not None:
            task_instance["id"] = ti_id
        run["facets"]["airflow"] = {"dagRun": dag_run, "taskInstance": task_instance}
        if loop_group is not None:
            run["facets"]["airflow"]["task"] = {"loop": {"group_id": loop_group}}
    elif job_type == "DAG":
        run["facets"]["airflowDagRun"] = {"dagRun": dag_run}
    return {
        "eventType": event_type,
        "job": {"name": job, "facets": {"jobType": {"jobType": job_type}}},
        "run": run,
    }


def _dag_event(run_id: str, event_type: str = "START", **kwargs) -> dict:
    return _event(run_id, event_type, job="dag", job_type="DAG", **kwargs)


def _query_event(run_id: str, event_type: str = "START", **kwargs) -> dict:
    return _event(run_id, event_type, job="dag.task.query.1", job_type="QUERY", **kwargs)


def _loop_events(
    run_id: str, loop_index: int, *, region_id="loop", loop_region_id="loop", trajectory=1, **kwargs
):
    """Build START and COMPLETE of a loop task's attempt, as the worker sends them."""
    return [
        _event(
            run_id,
            event_type,
            loop_index=loop_index,
            region_id=region_id,
            loop_group="body",
            loop_region_id=loop_region_id,
            trajectory=trajectory,
            ti_id=run_id,
            **kwargs,
        )
        for event_type in ("START", "COMPLETE")
    ]


VALID_SCENARIOS = {
    "start_and_complete": [_event("a"), _event("a", "COMPLETE")],
    "start_and_fail": [_event("a"), _event("a", "FAIL")],
    "retry": [
        _event("a"),
        _event("a", "FAIL"),
        _event("b", try_number=2),
        _event("b", "COMPLETE", try_number=2),
    ],
    "mapped": [
        _event("a", map_index=0),
        _event("a", "COMPLETE", map_index=0),
        _event("b", map_index=1),
        _event("b", "COMPLETE", map_index=1),
    ],
    "loop_iterations": [
        event
        for index, run_id in enumerate("abc")
        for event in (_event(run_id, loop_index=index), _event(run_id, "COMPLETE", loop_index=index))
    ],
    "mapped_in_loop": [
        event
        for run_id, map_index, loop_index in (("a", 0, 0), ("b", 1, 0), ("c", 0, 1), ("d", 1, 1))
        for event in (
            _event(run_id, map_index=map_index, loop_index=loop_index),
            _event(run_id, "COMPLETE", map_index=map_index, loop_index=loop_index),
        )
    ],
    "loop_iteration_run_again_after_a_clear": [
        _event("a", loop_index=1, region_id="loop_region"),
        _event("a", "COMPLETE", loop_index=1, region_id="loop_region"),
        _event("b", loop_index=1, region_id="forked_loop_region"),
        _event("b", "COMPLETE", loop_index=1, region_id="forked_loop_region"),
    ],
    "loop_task_iterations": [*_loop_events("a", 1), *_loop_events("b", 2)],
    "mapped_loop_task_iterations": [
        *_loop_events("a", 1, map_index=0, region_id="expansion_1"),
        *_loop_events("b", 2, map_index=0, region_id="expansion_2"),
    ],
    "loop_task_rerun_after_a_clear": [
        *_loop_events("a", 2),
        *_loop_events("b", 2, region_id="fork", loop_region_id="fork", trajectory=2),
    ],
    "same_task_in_two_dag_runs": [
        _event("a", dag_run_id="run_1"),
        _event("a", "COMPLETE", dag_run_id="run_1"),
        _event("b", dag_run_id="run_2"),
        _event("b", "COMPLETE", dag_run_id="run_2"),
    ],
    "query_start_and_complete": [_query_event("a"), _query_event("a", "COMPLETE")],
    "query_start_and_fail": [_query_event("a"), _query_event("a", "FAIL")],
    "dag_run_start_only": [_dag_event("a")],
    "dag_run_start_and_complete": [_dag_event("a"), _dag_event("a", "COMPLETE")],
    "dag_run_cleared": [
        _dag_event("a"),
        _dag_event("a", "FAIL"),
        _dag_event("b", clear_number=1),
        _dag_event("b", "COMPLETE", clear_number=1),
    ],
}

INVALID_SCENARIOS = {
    "start_and_complete_with_different_run_ids": (
        [_event("a"), _event("b", "COMPLETE")],
        "has several runIds",
    ),
    "complete_without_start": ([_event("a", "COMPLETE")], "one START"),
    "start_without_terminal": ([_event("a")], "one COMPLETE/FAIL"),
    "two_terminal_events": ([_event("a"), _event("a", "FAIL"), _event("a", "COMPLETE")], "one COMPLETE/FAIL"),
    "repeated_start_not_declared": (
        [_event("a"), _event("a"), _event("a", "COMPLETE")],
        "one START (several only if",
    ),
    "retry_reusing_run_id": (
        [_event("a"), _event("a", "FAIL"), _event("a", try_number=2), _event("a", "COMPLETE", try_number=2)],
        "shared by different attempts",
    ),
    "map_indexes_sharing_run_id": (
        [
            _event("a", map_index=0),
            _event("a", map_index=1),
            _event("a", "COMPLETE", map_index=0),
        ],
        "shared by different attempts",
    ),
    "loop_iterations_sharing_run_id": (
        [_event("a", loop_index=0), _event("a", "COMPLETE", loop_index=0), _event("a", loop_index=1)],
        "shared by different attempts",
    ),
    "loop_iteration_runs_of_two_trajectories_sharing_run_id": (
        [
            _event("a", loop_index=1, region_id="loop_region"),
            _event("a", "COMPLETE", loop_index=1, region_id="loop_region"),
            _event("a", loop_index=1, region_id="forked_loop_region"),
        ],
        "shared by different attempts",
    ),
    "loop_task_run_id_other_than_its_id": (
        [_event("a", loop_group="body", ti_id="b"), _event("a", "COMPLETE", loop_group="body", ti_id="b")],
        "isn't taskInstance.id",
    ),
    "loop_region_of_a_task_other_than_its_region": (
        _loop_events("a", 1, region_id="loop", loop_region_id="other"),
        "differs from its region_id",
    ),
    "loop_region_of_a_mapped_task_its_own_region": (
        _loop_events("a", 1, map_index=0, region_id="loop"),
        "of a mapped task is its own region_id",
    ),
    "loop_run_with_several_loop_regions": (
        [*_loop_events("a", 1), *_loop_events("b", 2, region_id="other", loop_region_id="other")],
        "has several loop.region_id",
    ),
    "dag_runs_sharing_run_id": (
        [_event("a", dag_run_id="run_1"), _event("a", "COMPLETE", dag_run_id="run_2")],
        "shared by different attempts",
    ),
    "clear_numbers_sharing_run_id": (
        [_dag_event("a"), _dag_event("a", "COMPLETE", clear_number=1)],
        "shared by different attempts",
    ),
    "dag_run_with_different_run_ids_per_event": (
        [_dag_event("a"), _dag_event("b", "COMPLETE")],
        "has several runIds",
    ),
    "query_start_without_terminal": ([_query_event("a")], "one COMPLETE/FAIL"),
    "query_complete_without_start": ([_query_event("a", "COMPLETE")], "one START"),
    "query_two_terminal_events": (
        [_query_event("a"), _query_event("a", "COMPLETE"), _query_event("a", "FAIL")],
        "one COMPLETE/FAIL",
    ),
    "dag_run_complete_without_start": ([_dag_event("a", "COMPLETE")], "one START and at most one"),
    "dag_run_two_terminal_events": (
        [_dag_event("a"), _dag_event("a", "COMPLETE"), _dag_event("a", "FAIL")],
        "one START and at most one",
    ),
    "jobs_sharing_run_id": (
        [_event("a", job="dag.one"), _event("a", "COMPLETE", job="dag.two")],
        "shared by different jobs",
    ),
    "missing_run_id": ([_event(None)], "has no run.runId"),
}


@pytest.mark.parametrize("events", VALID_SCENARIOS.values(), ids=VALID_SCENARIOS.keys())
def test_valid_events_have_no_problems(events):
    assert check_run_id_integrity({"dag.task.event.start": events}) == []


@pytest.mark.parametrize(
    ("events", "expected_problem"), INVALID_SCENARIOS.values(), ids=INVALID_SCENARIOS.keys()
)
def test_invalid_events_are_reported(events, expected_problem):
    problems = check_run_id_integrity({"dag.task.event.start": events})

    assert any(expected_problem in problem for problem in problems), problems


def test_events_of_one_attempt_are_joined_across_keys():
    events_by_key = {
        "dag.task.event.start": [_event("a")],
        "dag.task.event.complete": [_event("a", "COMPLETE")],
    }

    assert check_run_id_integrity(events_by_key) == []


@pytest.mark.parametrize(
    ("repeated_start_keys", "has_problems"),
    [((), True), (("dag.task.event.start",), False), (("other.event.start",), True)],
)
def test_repeated_starts_are_allowed_only_for_declared_keys(repeated_start_keys, has_problems):
    events_by_key = {
        "dag.task.event.start": [_event("a"), _event("a"), _event("a")],
        "dag.task.event.complete": [_event("a", "COMPLETE")],
    }

    problems = check_run_id_integrity(events_by_key, repeated_start_keys=repeated_start_keys)

    assert bool(problems) is has_problems


def test_repeated_starts_are_never_allowed_for_a_dag_run():
    events_by_key = {"dag.event.start": [_dag_event("a"), _dag_event("a")]}

    assert check_run_id_integrity(events_by_key, repeated_start_keys=("dag.event.start",))


@pytest.mark.parametrize(
    ("event_count_assertions", "expected"),
    [
        ({"dag.task.event.start": ">=2"}, True),
        ({"dag.task.event.start": ">=3"}, True),
        ({"dag.task.event.start": ">=1"}, False),
        ({"dag.task.event.start": "==2"}, False),
        ({"dag.task.event.start": 2}, False),
        ({".*\\.event\\.start": ">=2"}, True),
        ({"dag.task.event.complete": ">=2"}, False),
        (None, False),
    ],
)
def test_operator_derives_repeated_starts_from_event_count_assertions(event_count_assertions, expected):
    operator = OpenLineageTestOperator(
        task_id="check_events", event_templates={}, event_count_assertions=event_count_assertions
    )

    assert operator._expects_repeated_events("dag.task.event.start") is expected


@mock.patch("system.openlineage.operator.time.sleep", autospec=True)
@mock.patch.object(
    OpenLineageTestOperator, "_check_key", autospec=True, side_effect=ValueError("template mismatch")
)
@mock.patch("system.openlineage.operator.check_run_id_integrity", autospec=True)
def test_operator_skips_the_run_id_check_when_a_template_fails(check_integrity, _check_key, _sleep):
    operator = OpenLineageTestOperator(
        task_id="check_events", event_templates={"dag.task.event.start": {}}, clear_variables=False
    )

    with pytest.raises(ValueError, match="template mismatch") as err:
        operator.execute(context={})

    check_integrity.assert_not_called()
    assert "runId integrity" not in str(err.value)


SPLIT_ATTEMPT = {"dag.task.event.start": [_event("a")], "dag.task.event.complete": [_event("b", "COMPLETE")]}
RESUMED_ATTEMPT = {
    "dag.task.event.start": [_event("a"), _event("a")],
    "dag.task.event.complete": [_event("a", "COMPLETE")],
}


@pytest.mark.parametrize(
    ("events_by_key", "operator_kwargs", "expected_error"),
    [
        pytest.param(SPLIT_ATTEMPT, {}, r"Event check failures:\n  \[<runId integrity>\]", id="default"),
        pytest.param(SPLIT_ATTEMPT, {"fail_fast": True}, "^runId integrity check failed", id="fail_fast"),
        pytest.param(SPLIT_ATTEMPT, {"check_run_ids": False}, None, id="disabled"),
        pytest.param(
            RESUMED_ATTEMPT,
            {"event_count_assertions": {"dag.task.event.start": ">=2"}},
            None,
            id="declared_repeated_starts",
        ),
    ],
)
@mock.patch("system.openlineage.operator.time.sleep", autospec=True)
@mock.patch.object(OpenLineageTestOperator, "_check_key", autospec=True)
def test_operator_checks_the_run_ids_of_the_events_it_matched(
    check_key, _sleep, events_by_key, operator_kwargs, expected_error
):
    check_key.side_effect = lambda _operator, key, _template: events_by_key[key]
    operator = OpenLineageTestOperator(
        task_id="check_events",
        event_templates=dict.fromkeys(events_by_key, {}),
        clear_variables=False,
        **operator_kwargs,
    )

    if expected_error is None:
        operator.execute(context={})
    else:
        with pytest.raises(ValueError, match=expected_error):
            operator.execute(context={})


def test_operator_never_takes_its_retries_from_the_dag():
    operator = OpenLineageTestOperator(
        task_id="check_events", event_templates={}, default_args={"retries": 3}
    )

    assert operator.retries == 0
