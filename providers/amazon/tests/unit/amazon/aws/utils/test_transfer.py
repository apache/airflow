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

from airflow.providers.amazon.aws.utils.transfer import strip_overlapping_folder_markers


@pytest.mark.parametrize(
    ("keys", "expected_kept", "expected_dropped"),
    [
        ([], [], []),
        (["a"], ["a"], []),
        (["a", "b"], ["a", "b"], []),
        (["a", "ax"], ["a", "ax"], []),
        (["abc", "abcdef"], ["abc", "abcdef"], []),
        (["foo/", "foo/bar.txt"], ["foo/bar.txt"], ["foo/"]),
        (
            ["data/", "data/sub/", "data/sub/file.txt"],
            ["data/sub/file.txt"],
            ["data/", "data/sub/"],
        ),
        (["lonely/"], ["lonely/"], []),
        (["lonely/", "report.csv"], ["lonely/", "report.csv"], []),
    ],
)
def test_strip_overlapping_folder_markers(keys, expected_kept, expected_dropped):
    kept, dropped = strip_overlapping_folder_markers(keys)
    assert kept == expected_kept
    assert dropped == expected_dropped
