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

import pytest
from check_conf_import_in_providers import find_forbidden_conf_imports, is_excluded, is_executor_file


class TestIsExcluded:
    @pytest.mark.parametrize(
        "path, expected",
        [
            pytest.param(
                "providers/common/compat/src/airflow/providers/common/compat/sdk.py",
                True,
                id="compat-sdk-module",
            ),
            pytest.param(
                "providers/amazon/src/airflow/providers/amazon/hooks/s3.py",
                False,
                id="regular-provider-file",
            ),
        ],
    )
    def test_is_excluded(self, path: str, expected: bool):
        assert is_excluded(Path(path)) is expected


class TestIsExecutorFile:
    @pytest.mark.parametrize(
        "path, expected",
        [
            pytest.param(
                "providers/edge3/src/airflow/providers/edge3/executors/edge_executor.py",
                True,
                id="executor-file",
            ),
            pytest.param(
                "providers/celery/src/airflow/providers/celery/executors/celery_executor.py",
                True,
                id="celery-executor-file",
            ),
            pytest.param(
                "providers/amazon/src/airflow/providers/amazon/hooks/s3.py",
                False,
                id="regular-provider-file",
            ),
        ],
    )
    def test_is_executor_file(self, path: str, expected: bool):
        assert is_executor_file(Path(path)) is expected


class TestFindForbiddenConfImports:
    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param(
                "from airflow.configuration import conf\n",
                ["airflow.configuration.conf"],
                id="from-airflow-configuration",
            ),
            pytest.param(
                "from airflow.sdk.configuration import conf\n",
                ["airflow.sdk.configuration.conf"],
                id="from-airflow-sdk-configuration",
            ),
            pytest.param(
                "from airflow.configuration import conf as global_conf\n",
                ["airflow.configuration.conf"],
                id="aliased-conf",
            ),
            pytest.param(
                "def foo():\n    from airflow.configuration import conf\n",
                ["airflow.configuration.conf"],
                id="inside-function",
            ),
            pytest.param(
                "from __future__ import annotations\n"
                "from typing import TYPE_CHECKING\n"
                "if TYPE_CHECKING:\n"
                "    from airflow.sdk.configuration import conf\n",
                ["airflow.sdk.configuration.conf"],
                id="inside-type-checking",
            ),
        ],
    )
    def test_forbidden_imports(self, tmp_path: Path, code: str, expected: list[str]):
        f = tmp_path / "example.py"
        f.write_text(code)
        assert find_forbidden_conf_imports(f) == expected

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param("from airflow.providers.common.compat.sdk import conf\n", id="compat-sdk"),
            pytest.param("from airflow.configuration import has_option\n", id="other-config-attr"),
            pytest.param("from airflow.configuration import AirflowConfigParser\n", id="config-parser"),
            pytest.param("from airflow.providers.amazon.hooks.s3 import S3Hook\n", id="provider-import"),
            pytest.param("import os\nimport sys\n", id="stdlib-only"),
            pytest.param("x = 1\n", id="no-imports"),
        ],
    )
    def test_allowed_imports(self, tmp_path: Path, code: str):
        f = tmp_path / "example.py"
        f.write_text(code)
        assert find_forbidden_conf_imports(f) == []

    def test_executor_allows_airflow_configuration_conf(self, tmp_path: Path):
        executor_dir = tmp_path / "executors"
        executor_dir.mkdir()
        f = executor_dir / "my_executor.py"
        f.write_text("from airflow.configuration import conf\n")
        assert find_forbidden_conf_imports(f) == []

    def test_executor_still_forbids_sdk_configuration_conf(self, tmp_path: Path):
        executor_dir = tmp_path / "executors"
        executor_dir.mkdir()
        f = executor_dir / "my_executor.py"
        f.write_text("from airflow.sdk.configuration import conf\n")
        assert find_forbidden_conf_imports(f) == ["airflow.sdk.configuration.conf"]
