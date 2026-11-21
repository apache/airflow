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

import pytest
from ci.prek import update_retired_image_packages_defaults as hook

WORKFLOW = textwrap.dedent(
    """\
    on:
      workflow_dispatch:
        inputs:
          keep-branches:
            description: "Branches"
            type: string
            default: "main v3-3-test"
          keep-python:
            description: "Python versions"
            type: string
            default: "3.10 3.11"
          only:
            description: "Globs"
            type: string
            default: ""
    """
)


def test_find_maintained_branches_puts_main_first(tmp_path):
    for name in [
        "scheduled-upgrade-check-v3-4-test.yml",
        "scheduled-upgrade-check-main.yml",
        "scheduled-upgrade-check-v3-5-test.yml",
        "ci-amd.yml",
    ]:
        (tmp_path / name).touch()

    assert hook.find_maintained_branches(tmp_path) == ["main", "v3-4-test", "v3-5-test"]


def test_find_maintained_branches_requires_main(tmp_path):
    (tmp_path / "scheduled-upgrade-check-v3-4-test.yml").touch()

    with pytest.raises(RuntimeError, match="scheduled-upgrade-check-main.yml"):
        hook.find_maintained_branches(tmp_path)


def test_set_input_default_changes_only_that_input():
    updated = hook.set_input_default(WORKFLOW, "keep-python", "3.11 3.12")

    assert updated == WORKFLOW.replace('default: "3.10 3.11"', 'default: "3.11 3.12"')


def test_set_input_default_fails_for_a_missing_input():
    with pytest.raises(RuntimeError, match="'parallel'"):
        hook.set_input_default(WORKFLOW, "parallel", "8")


def test_workflow_defaults_are_in_sync():
    text = hook.WORKFLOW_PATH.read_text()
    expected = hook.set_input_default(
        text, "keep-branches", " ".join(hook.find_maintained_branches(hook.WORKFLOWS_PATH))
    )
    expected = hook.set_input_default(
        expected, "keep-python", " ".join(hook.read_allowed_python_major_minor_versions())
    )

    assert text == expected, "run 'prek run update-retired-image-packages-defaults --all-files'"
