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

from unittest import mock

import pytest
import requests

from airflow_breeze.utils import release_constraints


@pytest.fixture
def triggered(monkeypatch):
    """Capture what would have been dispatched, with the prompt answered yes."""
    calls: list[dict] = []
    monkeypatch.setattr(release_constraints, "confirm_action", lambda *a, **kw: True)
    monkeypatch.setattr(
        release_constraints, "trigger_workflow_and_monitor", lambda **kwargs: calls.append(kwargs)
    )
    return calls


@pytest.mark.parametrize(
    ("version", "ref"),
    [
        pytest.param("3.1.3rc1", "v3-1-stable", id="candidate"),
        pytest.param("3.1.3", "v3-1-stable", id="final"),
    ],
)
def test_version_and_ref_are_passed_through(triggered, version, ref):
    release_constraints.publish_constraints(version=version, ref=ref)

    assert len(triggered) == 1
    assert triggered[0]["version"] == version
    assert triggered[0]["ref"] == ref
    assert triggered[0]["workflow_name"] == "release-constraints.yml"
    assert triggered[0]["repo"] == "apache/airflow"


def test_workflow_definition_defaults_to_main(triggered):
    # Unlike the docs build, constraints need not be produced by the workflow as it stood at the
    # ref being released - so this defaults to main rather than to the ref.
    release_constraints.publish_constraints(version="3.1.3", ref="v3-1-stable")

    assert triggered[0]["branch"] == "main"


def test_workflow_definition_can_be_overridden(triggered):
    release_constraints.publish_constraints(
        version="3.1.3", ref="v3-1-stable", workflow_branch="my-fix-branch"
    )

    assert triggered[0]["branch"] == "my-fix-branch"


def test_nothing_is_dispatched_when_declined(monkeypatch, triggered):
    monkeypatch.setattr(release_constraints, "confirm_action", lambda *a, **kw: False)

    release_constraints.publish_constraints(version="3.1.3", ref="v3-1-stable")

    assert triggered == []


@pytest.mark.parametrize(
    ("version", "expected_stage"),
    [
        pytest.param("3.1.3rc2", "candidate", id="rc-is-a-candidate"),
        pytest.param("3.4.0b1", "beta", id="beta-is-a-beta"),
        pytest.param("3.1.3", "final", id="plain-version-is-final"),
    ],
)
def test_the_prompt_names_the_stage_the_version_implies(monkeypatch, version, expected_stage):
    """The stage is never passed separately, so the operator sees what the version implies."""
    prompts: list[str] = []
    monkeypatch.setattr(release_constraints, "confirm_action", lambda prompt, **kw: prompts.append(prompt))
    monkeypatch.setattr(release_constraints, "trigger_workflow_and_monitor", lambda **kw: None)

    release_constraints.publish_constraints(version=version, ref="v3-1-stable")

    assert expected_stage in prompts[0]


def _file(yanked: bool = False) -> dict:
    return {"yanked": yanked}


@pytest.mark.parametrize(
    ("releases", "expected"),
    [
        pytest.param({"1.0.0": [_file()], "2.0.0rc1": [_file()]}, "2.0.0rc1", id="candidate-newest"),
        pytest.param({"1.0.0rc1": [_file()], "1.0.0": [_file()]}, "1.0.0", id="candidate-released"),
        pytest.param({"1.0.0": [_file()], "2.0.0rc1": [_file(yanked=True)]}, "1.0.0", id="candidate-yanked"),
        pytest.param({"1.0.0": [_file()], "2.0.0": []}, "1.0.0", id="no-files"),
        pytest.param({"not-a-version": [_file()]}, None, id="nothing-usable"),
    ],
)
def test_find_newest_published_version(monkeypatch, releases, expected):
    response = mock.create_autospec(requests.Response, instance=True)
    response.status_code = 200
    response.json.return_value = {"releases": releases}
    monkeypatch.setattr(requests, "get", lambda *a, **kw: response)

    assert release_constraints.find_newest_published_version("apache-airflow-providers-x") == expected


def test_find_newest_published_version_unpublished(monkeypatch):
    response = mock.create_autospec(requests.Response, instance=True)
    response.status_code = 404
    monkeypatch.setattr(requests, "get", lambda *a, **kw: response)

    assert release_constraints.find_newest_published_version("apache-airflow-providers-x") is None


@mock.patch("airflow_breeze.utils.provider_dependencies.get_provider_dependencies")
def test_find_pending_provider_candidates_checks_only_ready_providers(get_deps, monkeypatch):
    get_deps.return_value = {
        "amazon": {"state": "ready"},
        "apache.hive": {"state": "ready"},
        "google": {"state": "ready"},
        "old": {"state": "suspended"},
        "new": {"state": "not-ready"},
    }
    newest = {
        "apache-airflow-providers-amazon": "10.0.0rc1",
        "apache-airflow-providers-apache-hive": "9.0.0",
        "apache-airflow-providers-google": None,
    }
    looked_up: list[str] = []

    def fake_lookup(distribution):
        looked_up.append(distribution)
        return newest[distribution]

    monkeypatch.setattr(release_constraints, "find_newest_published_version", fake_lookup)

    assert release_constraints.find_pending_provider_candidates() == [
        ("apache-airflow-providers-amazon", "10.0.0rc1")
    ]
    assert sorted(looked_up) == sorted(newest)


def test_exit_on_pending_provider_candidates_exits_when_pending(monkeypatch):
    monkeypatch.setattr(
        release_constraints,
        "find_pending_provider_candidates",
        lambda: [("apache-airflow-providers-amazon", "10.0.0rc1")],
    )

    with pytest.raises(SystemExit) as exc:
        release_constraints.exit_on_pending_provider_candidates()
    assert exc.value.code == 1


def test_exit_on_pending_provider_candidates_passes_when_clear(monkeypatch):
    monkeypatch.setattr(release_constraints, "find_pending_provider_candidates", lambda: [])

    release_constraints.exit_on_pending_provider_candidates()
