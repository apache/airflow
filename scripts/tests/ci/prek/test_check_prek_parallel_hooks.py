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

import pytest
from check_prek_parallel_hooks import PARALLEL_HOOKS, find_unaudited_priority_hooks, main

AUDITED_HOOK = sorted(PARALLEL_HOOKS)[0]

CONFIG = f"""\
repos:
  - repo: meta
    hooks:
      - id: no-priority
  - repo: local
    hooks:
      - id: {AUDITED_HOOK}
        priority: 1000
      - id: not-audited
        priority: 1000
      - id: {AUDITED_HOOK}
        priority: 50
      - id: alias
        priority: checks
"""


def test_find_unaudited_priority_hooks(tmp_path):
    config = tmp_path / ".pre-commit-config.yaml"
    config.write_text(CONFIG)

    assert find_unaudited_priority_hooks(config) == [
        "not-audited (priority 1000)",
        f"{AUDITED_HOOK} (priority 50)",
        "alias (priority 'checks')",
    ]


def test_find_unaudited_priority_hooks_empty_config(tmp_path):
    config = tmp_path / ".pre-commit-config.yaml"
    config.write_text("")

    assert find_unaudited_priority_hooks(config) == []


@pytest.mark.parametrize(
    ("config", "expected_returncode"),
    [
        pytest.param(CONFIG, 1, id="unaudited-priority"),
        pytest.param(
            f"repos:\n  - repo: local\n    hooks:\n      - id: {AUDITED_HOOK}\n        priority: 1000\n",
            0,
            id="audited-priority",
        ),
    ],
)
def test_main(tmp_path, monkeypatch, config, expected_returncode):
    (tmp_path / ".pre-commit-config.yaml").write_text(config)
    monkeypatch.chdir(tmp_path)

    assert main() == expected_returncode
