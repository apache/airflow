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

import textwrap
from pathlib import Path

import pytest
from check_coordinator_distributions import check_coordinator_distribution


@pytest.fixture
def coordinator_dist(tmp_path: Path) -> Path:
    dist = tmp_path / "java"
    (dist / "src" / "airflow" / "sdk" / "coordinators" / "java").mkdir(parents=True)
    (dist / "tests").mkdir()
    (dist / "LICENSE").write_text("license")
    (dist / "NOTICE").write_text("notice")
    (dist / "README.rst").write_text(
        textwrap.dedent(
            """\
            apache-airflow-coordinators-java
            =================================

            Installation
            ------------

            .. code-block:: bash

               pip install apache-airflow-coordinators-java
            """
        )
    )
    (dist / "pyproject.toml").write_text(
        textwrap.dedent(
            """\
            [project]
            name = "apache-airflow-coordinators-java"
            """
        )
    )
    return dist


def test_check_coordinator_distribution_accepts_valid_distribution(coordinator_dist: Path):
    assert check_coordinator_distribution(coordinator_dist) == []


def test_check_coordinator_distribution_reports_metadata_and_docs_errors(coordinator_dist: Path):
    (coordinator_dist / "pyproject.toml").write_text(
        textwrap.dedent(
            """\
            [project]
            name = "apache-airflow-coordinators-scala"
            """
        )
    )
    (coordinator_dist / "README.rst").write_text("apache-airflow-coordinators-java\n")
    (coordinator_dist / "NOTICE").unlink()

    errors = check_coordinator_distribution(coordinator_dist)

    assert "[project].name must be 'apache-airflow-coordinators-java'" in "\n".join(errors)
    assert "missing required file NOTICE" in "\n".join(errors)
    assert "missing installation instructions" in "\n".join(errors)
