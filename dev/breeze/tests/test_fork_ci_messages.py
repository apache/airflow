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

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from airflow_breeze.utils.fork_ci_messages import (
    _STATE_TEXTS,
    _STATUS_DESCRIPTIONS,
    BEHIND_WARNING_COMMITS,
    CI_PASSED_DESCRIPTION,
    CI_PASSED_REVOKED_DESCRIPTION,
    FOOTER,
    Marker,
    inactivity_close_comment,
    nudge_comment,
    parse_marker,
    released_comment,
    render_marker,
    state_comment,
    tamper_close_comment,
)

MARKER = Marker(state="awaiting_push", sha="abc123", since=datetime(2026, 10, 15, 10, 0, tzinfo=UTC))


@pytest.mark.parametrize(
    "marker",
    [pytest.param(MARKER, id="without-behind"), pytest.param(replace(MARKER, behind=12), id="behind")],
)
def test_marker_round_trip(marker):
    assert parse_marker(f"text before\n{render_marker(marker)}\ntext after") == marker


def test_marker_without_behind_keeps_its_rendering():
    assert "behind" not in render_marker(MARKER)


@pytest.mark.parametrize(
    ("state", "behind", "warned"),
    [
        pytest.param("green", BEHIND_WARNING_COMMITS, True, id="green-far-behind"),
        pytest.param("green_unverified", BEHIND_WARNING_COMMITS + 1, True, id="unverified-far-behind"),
        pytest.param("green", BEHIND_WARNING_COMMITS - 1, False, id="green-close"),
        pytest.param("green", None, False, id="green-not-counted"),
        pytest.param("failed", BEHIND_WARNING_COMMITS, False, id="failed"),
    ],
)
def test_state_comment_warns_about_a_branch_far_behind(state, behind, warned):
    text = state_comment(
        Marker(state, "a", MARKER.since, behind=behind), fork="u/airflow", run_url=None, base_ref="v3-1-test"
    )

    assert ("commits behind `v3-1-test`" in text) is warned


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
    ],
)
def test_every_comment_ends_with_footer(text):
    assert text.endswith(FOOTER)


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(state_comment(Marker("setup_required", "a", MARKER.since), fork="u/fork", run_url=None)),
        pytest.param(inactivity_close_comment(days=7, fork="u/fork")),
    ],
)
def test_setup_texts_give_the_exact_command(text):
    assert "gh workflow enable ci-amd.yml -R u/fork" in text


def test_status_descriptions_fit_github_limit():
    descriptions = [*_STATUS_DESCRIPTIONS.values(), CI_PASSED_DESCRIPTION, CI_PASSED_REVOKED_DESCRIPTION]
    assert all(len(description) < 140 for description in descriptions)
