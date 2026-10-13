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

import subprocess
from unittest import mock

import pytest

from docs.utils.example_source import example_source_ref


@pytest.mark.parametrize(
    ("outputs", "expected"),
    [
        pytest.param([""], "release-tag", id="head-unavailable"),
        pytest.param(
            ["head-sha", "head-sha"],
            "release-tag",
            id="head-matches-release-tag",
        ),
        pytest.param(
            ["head-sha", "tag-sha", "refs/remotes/upstream/main\n"],
            "head-sha",
            id="head-in-remote-branch",
        ),
        pytest.param(
            ["head-sha", "", "refs/tags/another-tag\n"],
            "head-sha",
            id="head-in-tag",
        ),
        pytest.param(
            ["head-sha", "tag-sha", ""],
            "release-tag",
            id="head-only-local",
        ),
    ],
)
def test_example_source_ref(outputs, expected):
    results = [subprocess.CompletedProcess(args=["git"], returncode=0, stdout=output) for output in outputs]

    with mock.patch(
        "docs.utils.example_source.subprocess.run",
        autospec=True,
        side_effect=results,
    ):
        assert example_source_ref("release-tag") == expected


def test_example_source_ref_without_git():
    with mock.patch(
        "docs.utils.example_source.subprocess.run",
        autospec=True,
        side_effect=OSError("Git is unavailable"),
    ):
        assert example_source_ref("release-tag") == "release-tag"
