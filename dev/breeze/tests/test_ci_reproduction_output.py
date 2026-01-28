# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.   The ASF licenses this file
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

import pytest
from click.testing import CliRunner

from airflow_breeze.breeze import main
from airflow_breeze.utils.shared_options import set_dry_run, set_verbose


@pytest.fixture(autouse=True)
def reset_shared_options():
    """Reset shared options before each test to ensure test isolation."""
    set_verbose(False)
    set_dry_run(False)
    yield
    set_verbose(False)
    set_dry_run(False)


def test_selective_check_prints_reproduction_in_ci_verbose(monkeypatch):
    monkeypatch.setenv("CI", "true")

    runner = CliRunner(mix_stderr=True, env={"CI": "true"})

    result = runner.invoke(
        main,
        [
            "ci",
            "selective-check",
            "--verbose",
            "--github-event-name",
            "pull_request",
            "--github-repository",
            "apache/airflow",
            "--github-context",
            "{}",
        ],
    )

    assert result.exit_code == 0
    assert "HOW TO REPRODUCE LOCALLY" in result.output
    assert "breeze ci selective-check --verbose" in result.output


def test_selective_check_does_not_print_reproduction_without_verbose(monkeypatch):
    monkeypatch.setenv("CI", "true")

    runner = CliRunner(mix_stderr=True, env={"CI": "true"})

    result = runner.invoke(
        main,
        [
            "ci",
            "selective-check",
            "--github-event-name",
            "pull_request",
            "--github-repository",
            "apache/airflow",
            "--github-context",
            "{}",
        ],
    )

    assert result.exit_code == 0
    assert "HOW TO REPRODUCE LOCALLY" not in result.output


def test_selective_check_does_not_print_reproduction_outside_ci(monkeypatch):
    monkeypatch.delenv("CI", raising=False)

    runner = CliRunner(mix_stderr=True, env={})

    result = runner.invoke(
        main,
        [
            "ci",
            "selective-check",
            "--verbose",
            "--github-event-name",
            "pull_request",
            "--github-repository",
            "apache/airflow",
            "--github-context",
            "{}",
        ],
    )

    assert result.exit_code == 0
    assert "HOW TO REPRODUCE LOCALLY" not in result.output