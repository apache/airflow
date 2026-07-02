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
from ci.prek.check_eval_reminder import find_guidance_files, main


@pytest.mark.parametrize(
    ("staged", "expected"),
    [
        pytest.param([], [], id="empty"),
        pytest.param(["AGENTS.md"], ["AGENTS.md"], id="root-agents"),
        pytest.param(["providers/AGENTS.md"], ["providers/AGENTS.md"], id="nested-agents"),
        pytest.param(
            [".agents/skills/airflow-translations/SKILL.md"],
            [".agents/skills/airflow-translations/SKILL.md"],
            id="nested-skill",
        ),
        pytest.param(["airflow-core/src/airflow/models/dag.py", "README.md"], [], id="unrelated"),
        pytest.param(["MYAGENTS.md", "docs/OLDSKILL.md"], [], id="basename-must-match-exactly"),
        pytest.param(
            ["AGENTS.md", "task-sdk/AGENTS.md", "dev/foo.py"],
            ["AGENTS.md", "task-sdk/AGENTS.md"],
            id="mixed",
        ),
    ],
)
def test_find_guidance_files(staged, expected):
    assert find_guidance_files(staged) == expected


@mock.patch("ci.prek.check_eval_reminder.get_staged_files", autospec=True)
def test_main_prints_reminder_for_guidance_files(mock_get_staged_files, capsys):
    mock_get_staged_files.return_value = ["AGENTS.md", "dev/foo.py"]

    assert main() == 0

    err = capsys.readouterr().err
    assert "AGENTS.md" in err
    assert "dev/skill-evals/eval.py" in err


@mock.patch("ci.prek.check_eval_reminder.get_staged_files", autospec=True)
def test_main_is_silent_and_passes_without_guidance_files(mock_get_staged_files, capsys):
    mock_get_staged_files.return_value = ["airflow-core/src/airflow/models/dag.py"]

    assert main() == 0

    assert capsys.readouterr().err == ""
