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

import re
import subprocess
import sys
from unittest import mock

import check_go_sdk_serialization_conformance as checker


@mock.patch("check_go_sdk_serialization_conformance.subprocess.run", autospec=True)
def test_main_runs_compare_with_the_go_serializer_and_returns_its_exit_code(mock_run):
    mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=1)

    assert checker.main() == 1

    command = mock_run.call_args.args[0]
    assert command == [sys.executable, str(checker.COMPARE), "--sdk", "go", "--", *checker.SERIALIZER]
    assert mock_run.call_args.kwargs["cwd"] == checker.REPO_ROOT


def test_the_serializer_runs_exactly_the_go_test_that_writes_the_dags():
    """If the Go test were renamed, the hook would run no test, and compare.py would fail on a missing file."""
    serializer = checker.SERIALIZER
    package = (
        checker.REPO_ROOT / serializer[serializer.index("-C") + 1] / serializer[serializer.index("test") + 1]
    )
    run_pattern = serializer[serializer.index("-run") + 1]
    test_names = [
        name
        for test_file in package.glob("*_test.go")
        for name in re.findall(r"^func (Test\w+)\(t \*testing\.T\)", test_file.read_text(), re.MULTILINE)
    ]

    assert [name for name in test_names if re.search(run_pattern, name)] == ["TestSerializeConformanceDags"]
    # compare.py appends the two paths, which reach the test only after -args.
    assert serializer[-1] == "-args"
