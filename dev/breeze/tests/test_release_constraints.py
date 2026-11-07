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


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        pytest.param(None, "", id="derive-from-version"),
        pytest.param(False, "false", id="pin-released-providers"),
        pytest.param(True, "true", id="force-guard"),
    ],
)
def test_override_is_passed_to_the_workflow_input(triggered, override, expected):
    release_constraints.publish_constraints(
        version="3.4.0", ref="v3-4-stable", fail_on_pending_provider_candidates=override
    )

    assert triggered[0]["fail-on-pending-provider-candidates"] == expected


@pytest.mark.parametrize(
    ("releases", "expected"),
    [
        pytest.param({"1.0.0": [{}], "2.0.0rc1": [{}]}, "2.0.0rc1", id="candidate-newest"),
        pytest.param({"1.0.0rc1": [{}], "1.0.0": [{}]}, "1.0.0", id="candidate-released"),
        pytest.param({"1.0.0": [{}], "2.0.0rc1": [{"yanked": True}]}, "1.0.0", id="candidate-yanked"),
        pytest.param({"1.0.0": [{}], "2.0.0": []}, "1.0.0", id="no-files"),
        pytest.param({"not-a-version": [{}]}, None, id="nothing-usable"),
    ],
)
@mock.patch.object(requests, "get", autospec=True)
def test_find_newest_published_version(mock_get, releases, expected):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"releases": releases}

    assert release_constraints._find_newest_published_version("apache-airflow-providers-x") == expected


@mock.patch.object(requests, "get", autospec=True)
def test_find_newest_published_version_unpublished(mock_get):
    mock_get.return_value.status_code = 404

    assert release_constraints._find_newest_published_version("apache-airflow-providers-x") is None


@mock.patch("airflow_breeze.utils.provider_dependencies.get_provider_dependencies", autospec=True)
def test_find_pending_provider_candidates_checks_only_ready_providers(mock_deps, monkeypatch):
    mock_deps.return_value = {
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
    monkeypatch.setattr(release_constraints, "_find_newest_published_version", newest.__getitem__)

    assert release_constraints.find_pending_provider_candidates() == [
        ("apache-airflow-providers-amazon", "10.0.0rc1")
    ]
