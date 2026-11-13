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

from pathlib import Path
from unittest import mock

import pytest

from airflow.models.errors import ParseImportError

BUNDLE_PATH = Path("/bundle")


@pytest.fixture
def bundle_path():
    with mock.patch("airflow.models.errors.DagBundlesManager", autospec=True) as manager:
        manager.return_value.get_bundle.return_value.path = BUNDLE_PATH
        yield


class TestFullFilePath:
    @pytest.mark.usefixtures("bundle_path")
    @pytest.mark.parametrize(
        ("source_reference", "expected"),
        [
            pytest.param("dags/my_dag.py", "/bundle/dags/my_dag.py", id="relative-file"),
            pytest.param(
                "archive.zip/dags/my_dag.py",
                "/bundle/archive.zip/dags/my_dag.py",
                id="relative-archive-member",
            ),
            pytest.param(
                "/elsewhere/archive.zip/dags/my_dag.py",
                "/elsewhere/archive.zip/dags/my_dag.py",
                id="absolute-archive-member",
            ),
            pytest.param("/bundle/dags/my_dag.py", "/bundle/dags/my_dag.py", id="already-under-bundle"),
            pytest.param(
                "/bundle-sibling/dags/my_dag.py",
                "/bundle-sibling/dags/my_dag.py",
                id="sibling-bundle-prefix",
            ),
        ],
    )
    def test_resolves_reference_against_bundle(self, source_reference, expected):
        error = ParseImportError(bundle_name="my-bundle", source_reference=source_reference)

        assert error.full_file_path() == expected

    @pytest.mark.usefixtures("bundle_path")
    def test_resolves_filename_when_source_reference_is_none(self):
        error = ParseImportError(bundle_name="my-bundle", filename="dags/my_dag.py")

        assert error.full_file_path() == "/bundle/dags/my_dag.py"

    @pytest.mark.parametrize(
        ("bundle_name", "source_reference", "filename"),
        [
            pytest.param(None, "dags/my_dag.py", None, id="missing-bundle"),
            pytest.param("my-bundle", None, None, id="missing-reference"),
        ],
    )
    def test_raises_when_reference_is_incomplete(self, bundle_name, source_reference, filename):
        error = ParseImportError(
            bundle_name=bundle_name, source_reference=source_reference, filename=filename
        )

        with pytest.raises(
            ValueError, match=r"bundle_name and \(source_reference or filename\) must not be None"
        ):
            error.full_file_path()
