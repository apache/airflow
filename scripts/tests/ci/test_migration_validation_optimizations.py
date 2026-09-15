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

import itertools
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
UNIT_WORKFLOW = ".github/workflows/run-unit-tests.yml"


def load_yaml(path):
    return yaml.load((ROOT / path).read_text(), Loader=yaml.BaseLoader)


def test_migrations_stay_in_an_existing_required_test_check():
    jobs = load_yaml(UNIT_WORKFLOW)["jobs"]
    assert set(jobs) == {"tests"}
    tests = jobs["tests"]
    assert "continue-on-error" not in tests
    assert tests["strategy"]["fail-fast"] == "false"
    step = next(step for step in tests["steps"] if step.get("uses", "").endswith("/migration_tests"))
    assert step["with"]["python-version"] == "${{ matrix.python-version }}"
    condition = step["if"]
    assert "inputs.run-migration-tests == 'true'" in condition
    assert "inputs.test-group == 'core'" in condition
    assert "matrix.python-version != '3.14'" in condition
    assert (
        "matrix.test-types.description == fromJSON(inputs.test-types-as-strings-in-json)[0].description"
        in condition
    )
    assert "continue-on-error" not in step


@pytest.mark.parametrize("python_versions", [("3.10",), ("3.14",), ("3.10", "3.12", "3.14")])
@pytest.mark.parametrize("backend_versions", [("12",), ("12", "16")])
@pytest.mark.parametrize(
    "excluded",
    [[], [{"python-version": "3.12", "backend-version": "12"}], [{"backend-version": "16"}]],
)
def test_migration_projection_keeps_each_eligible_environment_once(
    python_versions, backend_versions, excluded
):
    original = [
        {"python-version": py, "backend-version": db, "test-types": shard}
        for py, db, shard in itertools.product(python_versions, backend_versions, ("A", "B"))
    ]
    retained = [
        row
        for row in original
        if row["python-version"] != "3.14"
        and not any(all(row.get(key) == value for key, value in entry.items()) for entry in excluded)
    ]
    expected = {(row["python-version"], row["backend-version"]) for row in retained}
    actual = [(row["python-version"], row["backend-version"]) for row in retained if row["test-types"] == "A"]
    assert set(actual) == expected
    assert len(actual) == len(set(actual))
