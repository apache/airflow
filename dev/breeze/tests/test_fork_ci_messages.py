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

import functools
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from airflow_breeze.utils.fork_ci_messages import (
    _STATE_TEXTS,
    _STATUS_DESCRIPTIONS,
    CI_CHANGE_MARKER,
    CI_MODIFICATION_MARKER,
    CI_PASSED_REVOKED_DESCRIPTION,
    FOOTER,
    GATED_NOTE_SENTENCE,
    JOB_NAME_MAX_LENGTH,
    JOB_STATUS_CONTEXT_PREFIX,
    MAX_BEHIND_COMMITS,
    MAX_LISTED_CI_FILES,
    OVERLAP_LISTED,
    BaseCheck,
    Marker,
    ci_change_approval_comment,
    ci_modification_note_comment,
    ci_passed_description,
    gate_modified_close_comment,
    inactivity_close_comment,
    job_status_context,
    nudge_comment,
    parse_marker,
    rebase_needed_description,
    released_comment,
    render_marker,
    state_comment,
    tamper_close_comment,
    ungated_comment,
    workflow_disabled_comment,
)

MARKER = Marker(state="awaiting_push", sha="abc123", since=datetime(2026, 10, 15, 10, 0, tzinfo=UTC))


@pytest.mark.parametrize(
    "marker",
    [
        pytest.param(MARKER, id="without-base"),
        pytest.param(replace(MARKER, base=BaseCheck("def456", 12, ("a.py",))), id="base"),
        pytest.param(replace(MARKER, base=BaseCheck("def456", 12, None)), id="base-unchecked"),
        pytest.param(replace(MARKER, jobs="123:SUCCESS"), id="jobs"),
        pytest.param(replace(MARKER, gate_checked=True), id="gate-checked"),
    ],
)
def test_marker_round_trip(marker):
    assert parse_marker(f"text before\n{render_marker(marker)}\ntext after") == marker


def test_marker_without_base_keeps_its_rendering():
    assert "base" not in render_marker(MARKER)
    assert "jobs" not in render_marker(MARKER)


def test_marker_from_before_the_base_check_has_no_base():
    marker = parse_marker(
        '<!-- fork-ci-gate: {"behind": 3, "sha": "a", "since": "2026-10-15T10:00:00+00:00", "state": "green"} -->'
    )

    assert marker is not None
    assert marker.base is None


@pytest.mark.parametrize(
    ("base", "current"),
    [
        pytest.param(BaseCheck("d", MAX_BEHIND_COMMITS - 1), True, id="close-no-overlap"),
        pytest.param(BaseCheck("d", MAX_BEHIND_COMMITS), False, id="far-behind"),
        pytest.param(BaseCheck("d", 1, ("a.py",)), False, id="overlap"),
        pytest.param(BaseCheck("d", 1, None), False, id="unchecked"),
    ],
)
def test_base_check_is_current(base, current):
    assert base.is_current is current


@pytest.mark.parametrize(
    ("state", "base", "expected", "unexpected"),
    [
        pytest.param(
            "green",
            BaseCheck("d" * 40, 3),
            [
                "`v3-1-test` at [`dddddddddd`](https://github.com/apache/airflow/commit/" + "d" * 40,
                "3 commits",
            ],
            ["Rebase needed"],
            id="current",
        ),
        pytest.param(
            "green",
            BaseCheck("d", MAX_BEHIND_COMMITS),
            ["Rebase needed", "too far behind", "git rebase upstream/v3-1-test"],
            [],
            id="far-behind",
        ),
        pytest.param(
            "green_unverified",
            BaseCheck("d", 4, ("a.py", "b.py")),
            ["Rebase needed", "`a.py`, `b.py`."],
            [],
            id="overlap",
        ),
        pytest.param(
            "green",
            BaseCheck("d", 4, tuple(f"f{i:02}.py" for i in range(OVERLAP_LISTED + 2))),
            [f"`f{OVERLAP_LISTED - 1:02}.py` and 2 more."],
            [f"`f{OVERLAP_LISTED:02}.py`"],
            id="overlap-shortened",
        ),
        pytest.param(
            "green", BaseCheck("d", 4, None), ["Rebase needed", "too many files"], [], id="unchecked"
        ),
        pytest.param("failed", BaseCheck("d", MAX_BEHIND_COMMITS), [], ["tested this branch"], id="failed"),
        pytest.param("green", None, [], ["tested this branch"], id="not-compared"),
    ],
)
def test_state_comment_says_which_base_was_tested(state, base, expected, unexpected):
    text = state_comment(
        Marker(state, "a", MARKER.since, base=base), fork="u/airflow", run_url=None, base_ref="v3-1-test"
    )

    assert all(part in text for part in expected)
    assert not any(part in text for part in unexpected)


@pytest.mark.parametrize(
    ("state", "author_draft", "asks_to_mark_ready"),
    [
        pytest.param("green", True, True, id="green-author-draft"),
        pytest.param("green_unverified", True, True, id="unverified-author-draft"),
        pytest.param("green", False, False, id="green-ready"),
        pytest.param("failed", True, False, id="failed-author-draft"),
    ],
)
def test_state_comment_asks_the_author_to_mark_a_green_draft_ready(state, author_draft, asks_to_mark_ready):
    text = state_comment(
        Marker(state, "a", MARKER.since), fork="u/airflow", run_url=None, author_draft=author_draft
    )

    assert ("mark it ready for review" in text.lower()) is asks_to_mark_ready


def test_parse_marker_absent():
    assert parse_marker("just a comment") is None


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("<!-- fork-ci-gate: not json -->", id="not-json"),
        pytest.param('<!-- fork-ci-gate: {"state": "green"} -->', id="missing-keys"),
        pytest.param(
            '<!-- fork-ci-gate: {"state": "green", "sha": "a", "since": "never"} -->', id="bad-date"
        ),
    ],
)
def test_parse_marker_malformed(body):
    with pytest.raises(ValueError, match="Malformed fork CI marker"):
        parse_marker(body)


@pytest.mark.parametrize(
    "text",
    [
        *(
            pytest.param(
                state_comment(Marker(state, "a", MARKER.since), fork="u/airflow", run_url=None), id=state
            )
            for state in _STATE_TEXTS
        ),
        pytest.param(released_comment(MARKER), id="released"),
        pytest.param(nudge_comment(fork="u/airflow"), id="nudge"),
        pytest.param(inactivity_close_comment(days=7, fork="u/airflow"), id="inactivity"),
        pytest.param(tamper_close_comment(run_url="https://x"), id="tamper"),
        pytest.param(
            workflow_disabled_comment(fork="u/airflow", workflow_state="disabled_manually"), id="disabled"
        ),
        pytest.param(gate_modified_close_comment(change="it changes `x`"), id="gate-modified"),
        pytest.param(ungated_comment(MARKER, base_ref="v2-10-test"), id="ungated"),
    ],
)
def test_every_comment_ends_with_footer(text):
    assert text.endswith(FOOTER)


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(state_comment(Marker("setup_required", "a", MARKER.since), fork="u/fork", run_url=None)),
        pytest.param(inactivity_close_comment(days=7, fork="u/fork")),
        pytest.param(workflow_disabled_comment(fork="u/fork", workflow_state="disabled_manually")),
    ],
)
def test_setup_texts_give_the_exact_command(text):
    assert "gh repo sync u/fork --branch main" in text
    assert "gh workflow enable ci-amd.yml -R u/fork" in text


def test_status_descriptions_fit_github_limit():
    base_ref = "providers-cncf-kubernetes/v10-22"
    descriptions = [
        *_STATUS_DESCRIPTIONS.values(),
        CI_PASSED_REVOKED_DESCRIPTION,
        ci_passed_description(BaseCheck("d" * 40, 199), base_ref=base_ref),
        *(
            rebase_needed_description(BaseCheck("d" * 40, 9999, overlap), base_ref=base_ref)
            for overlap in ((), ("a",) * 300, None)
        ),
    ]
    assert all(len(description) < 140 for description in descriptions)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Static checks", "Static checks"),
        ("a\nb\tc", "a b c"),
        ("x" * (JOB_NAME_MAX_LENGTH + 5), "x" * (JOB_NAME_MAX_LENGTH - 1) + "…"),
    ],
)
def test_job_status_context(name, expected):
    assert job_status_context(name) == f"{JOB_STATUS_CONTEXT_PREFIX}{expected}"


@pytest.mark.parametrize(
    ("build_comment", "marker"),
    [
        pytest.param(ci_change_approval_comment, CI_CHANGE_MARKER, id="approval"),
        pytest.param(ci_modification_note_comment, CI_MODIFICATION_MARKER, id="note"),
    ],
)
def test_ci_file_comments_list_the_files(build_comment, marker):
    if build_comment is ci_modification_note_comment:
        build_comment = functools.partial(build_comment, gated=False)
    paths = [f".github/workflows/w{i}.yml" for i in range(MAX_LISTED_CI_FILES + 2)]
    comment = build_comment(ci_files=["dev/breeze/`evil`.py", ".github/a\n\n@someone.yml", *paths])

    assert comment.startswith(marker)
    assert comment.endswith(FOOTER)
    assert "- `dev/breeze/?evil?.py`" in comment
    assert "- `.github/a??@someone.yml`" in comment
    assert "\n@someone" not in comment
    assert "- and 4 more" in comment
    assert f"`{paths[-1]}`" not in comment


@pytest.mark.parametrize("gated", [True, False])
def test_ci_modification_note_mentions_fork_execution_only_when_gated(gated):
    comment = ci_modification_note_comment(ci_files=["Dockerfile.ci"], gated=gated)

    assert (GATED_NOTE_SENTENCE in comment) is gated
    assert comment.endswith(FOOTER)
