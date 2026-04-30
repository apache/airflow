#
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

import uuid
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from airflow._shared.workloads import TaskInstanceDTO
from airflow.providers.sdk.java.bundle_scanner import (
    MAIN_CLASS_MANIFEST_KEY,
    MANIFEST_PATH,
    METADATA_MANIFEST_KEY,
    SDK_VERSION_MANIFEST_KEY,
)
from airflow.providers.sdk.java.coordinator import JavaCoordinator
from airflow.sdk.api.datamodels._generated import BundleInfo

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)

METADATA_YAML_PATH = "META-INF/airflow-metadata.yaml"
DAG_CODE_PATH = "dag_source.py"
TEST_MAIN_CLASS = "com.example.MyBundle"


def _make_manifest(
    *,
    main_class: str | None = TEST_MAIN_CLASS,
    metadata_path: str | None = METADATA_YAML_PATH,
    dag_code_path: str | None = None,
) -> str:
    lines = ["Manifest-Version: 1.0"]
    if main_class:
        lines.append(f"{MAIN_CLASS_MANIFEST_KEY}: {main_class}")
    if metadata_path:
        lines.append(f"{METADATA_MANIFEST_KEY}: {metadata_path}")
    lines.append(f"{SDK_VERSION_MANIFEST_KEY}: 1.0.0")
    if dag_code_path:
        lines.append(f"Airflow-Java-SDK-Dag-Code: {dag_code_path}")
    return "\n".join(lines) + "\n"


def _create_bundle_jar(
    jar_path: Path,
    *,
    dag_ids: list[str] | None = None,
    dag_code: str | None = None,
) -> Path:
    with zipfile.ZipFile(jar_path, "w") as zf:
        dag_code_path = DAG_CODE_PATH if dag_code else None
        manifest = _make_manifest(dag_code_path=dag_code_path)
        zf.writestr(MANIFEST_PATH, manifest)
        if dag_ids is not None:
            metadata = yaml.dump({"dags": {d: {} for d in dag_ids}})
            zf.writestr(METADATA_YAML_PATH, metadata)
        if dag_code:
            zf.writestr(DAG_CODE_PATH, dag_code)
    return jar_path


def _make_ti(dag_id: str = "test_dag") -> TaskInstanceDTO:
    return TaskInstanceDTO(
        id=uuid.uuid4(),
        dag_version_id=uuid.uuid4(),
        task_id="task_1",
        dag_id=dag_id,
        run_id="run_1",
        try_number=1,
        map_index=-1,
        pool_slots=1,
        queue="default",
        priority_weight=1,
    )


class TestJavaCoordinatorAttributes:
    def test_sdk(self):
        assert JavaCoordinator.sdk == "java"

    def test_file_extension(self):
        assert JavaCoordinator.file_extension == ".jar"


class TestCanHandleDagFile:
    def test_valid_jar_returns_true(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "valid.jar", dag_ids=["d"])
        assert JavaCoordinator.can_handle_dag_file("bundle", str(jar)) is True

    def test_non_jar_file_returns_false(self, tmp_path: Path):
        py_file = tmp_path / "dag.py"
        py_file.write_text("from airflow import DAG")
        assert JavaCoordinator.can_handle_dag_file("bundle", str(py_file)) is False

    def test_missing_file_returns_false(self, tmp_path: Path):
        assert JavaCoordinator.can_handle_dag_file("bundle", str(tmp_path / "missing.jar")) is False

    def test_bad_zip_returns_false(self, tmp_path: Path):
        bad = tmp_path / "bad.jar"
        bad.write_text("not a zip")
        assert JavaCoordinator.can_handle_dag_file("bundle", str(bad)) is False

    def test_jar_without_sdk_manifest_returns_false(self, tmp_path: Path):
        jar = tmp_path / "plain.jar"
        with zipfile.ZipFile(jar, "w") as zf:
            zf.writestr("dummy.class", b"")
        assert JavaCoordinator.can_handle_dag_file("bundle", str(jar)) is False


class TestGetCodeFromFile:
    def test_returns_embedded_code(self, tmp_path: Path):
        code = "from airflow import DAG\ndag = DAG('my_dag')"
        jar = _create_bundle_jar(tmp_path / "with_code.jar", dag_ids=["d"], dag_code=code)
        assert JavaCoordinator.get_code_from_file(str(jar)) == code

    def test_raises_when_no_code(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "no_code.jar", dag_ids=["d"])
        with pytest.raises(FileNotFoundError, match="No DAG source code found in JAR"):
            JavaCoordinator.get_code_from_file(str(jar))


class TestDagParsingCmd:
    def test_builds_java_command(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "app.jar", dag_ids=["d"])
        bundle_path = str(tmp_path)
        cmd = JavaCoordinator.dag_parsing_cmd(
            dag_file_path=str(jar),
            bundle_name="my_bundle",
            bundle_path=bundle_path,
            comm_addr="localhost:1234",
            logs_addr="localhost:5678",
        )
        assert cmd == [
            "java",
            "-classpath",
            f"{bundle_path}/*",
            TEST_MAIN_CLASS,
            "--comm=localhost:1234",
            "--logs=localhost:5678",
        ]


class TestTaskExecutionCmd:
    def test_pure_java_dag(self, tmp_path: Path):
        jar = _create_bundle_jar(tmp_path / "app.jar", dag_ids=["test_dag"])
        bundle_path = str(tmp_path)
        ti = _make_ti()
        bundle_info = BundleInfo(name="my_bundle")

        cmd = JavaCoordinator.task_execution_cmd(
            what=ti,  # type: ignore[arg-type]
            dag_file_path=str(jar),
            bundle_path=bundle_path,
            bundle_info=bundle_info,
            comm_addr="localhost:1234",
            logs_addr="localhost:5678",
        )
        assert cmd == [
            "java",
            "-classpath",
            f"{bundle_path}/*",
            TEST_MAIN_CLASS,
            "--comm=localhost:1234",
            "--logs=localhost:5678",
        ]

    def test_python_stub_dag_with_bundles_folder(self, tmp_path: Path):
        bundles_folder = tmp_path / "java_bundles"
        bundle_sub = bundles_folder / "my_bundle"
        bundle_sub.mkdir(parents=True)
        _create_bundle_jar(bundle_sub / "app.jar", dag_ids=["stub_dag"])

        ti = _make_ti(dag_id="stub_dag")
        bundle_info = BundleInfo(name="my_bundle")

        with patch(
            "airflow.providers.common.compat.sdk.conf.get",
            return_value=str(bundles_folder),
        ):
            cmd = JavaCoordinator.task_execution_cmd(
                what=ti,  # type: ignore[arg-type]
                dag_file_path="/dags/stub_dag.py",
                bundle_path="/some/bundle/path",
                bundle_info=bundle_info,
                comm_addr="localhost:1234",
                logs_addr="localhost:5678",
            )

            assert cmd == [
                "java",
                "-classpath",
                f"{bundles_folder}/my_bundle/app.jar",
                TEST_MAIN_CLASS,
                "--comm=localhost:1234",
                "--logs=localhost:5678",
            ]

    @pytest.mark.parametrize(
        "config_value",
        [
            pytest.param(None, id="none"),
            pytest.param("", id="empty_string"),
        ],
    )
    def test_python_stub_dag_invalid_config_raises(self, config_value):
        ti = _make_ti()
        bundle_info = BundleInfo(name="my_bundle")

        with patch(
            "airflow.providers.common.compat.sdk.conf.get",
            return_value=config_value,
        ):
            with pytest.raises(ValueError, match="bundles_folder config must be set"):
                JavaCoordinator.task_execution_cmd(
                    what=ti,  # type: ignore[arg-type]
                    dag_file_path="/dags/stub_dag.py",
                    bundle_path="/some/bundle/path",
                    bundle_info=bundle_info,
                    comm_addr="localhost:1234",
                    logs_addr="localhost:5678",
                )
