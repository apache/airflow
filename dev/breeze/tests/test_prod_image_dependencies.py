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

import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path
from unittest import mock

import pytest

from airflow_breeze.commands.production_image_commands import clean_docker_context_files
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.docker_command_utils import prepare_docker_build_command
from airflow_breeze.utils.prod_image_dependencies import prepare_dependency_context, write_metadata_wheel


@pytest.fixture
def context(tmp_path: Path) -> Path:
    wheel = tmp_path / "apache_airflow-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "apache_airflow-1.0.dist-info/METADATA",
            "Metadata-Version: 2.3\nName: apache-airflow\nVersion: 1.0\n"
            'Requires-Dist: example-dependency; extra == "test"\n'
            "Provides-Extra: test\nRequires-Python: >=3.10\n",
        )
        archive.writestr(
            "apache_airflow-1.0.dist-info/WHEEL",
            "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr("airflow/__init__.py", "SOURCE = 'first'\n")
    return tmp_path


class TestMetadataWheels:
    def test_preserves_resolver_inputs_and_valid_record(self, context: Path):
        source = context / "apache_airflow-1.0-py3-none-any.whl"
        output = context / "metadata.whl"
        write_metadata_wheel(source, output)
        with zipfile.ZipFile(source) as original, zipfile.ZipFile(output) as metadata:
            assert len(metadata.namelist()) == 3
            for name in metadata.namelist():
                if not name.endswith("/RECORD"):
                    assert metadata.read(name) == original.read(name)
            record = metadata.read("apache_airflow-1.0.dist-info/RECORD").decode()
            for name, digest, size in csv.reader(io.StringIO(record)):
                if digest:
                    content = metadata.read(name)
                    assert int(size) == len(content)
                    assert (
                        digest
                        == "sha256="
                        + base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=").decode()
                    )

    def test_source_changes_do_not_invalidate_metadata(self, context: Path):
        destination = prepare_dependency_context(context, "3.10")
        wheel_name = "apache_airflow-1.0-py3-none-any.whl"
        before = (destination / wheel_name).read_bytes()
        with zipfile.ZipFile(context / wheel_name, "a") as archive:
            archive.writestr("airflow/new_module.py", "SOURCE = 'second'\n")
        prepare_dependency_context(context, "3.10")
        assert (destination / wheel_name).read_bytes() == before

    @pytest.mark.parametrize("metadata_file", ["METADATA", "WHEEL"])
    def test_dependency_and_compatibility_changes_invalidate_metadata(
        self, context: Path, metadata_file: str
    ):
        destination = prepare_dependency_context(context, "3.10")
        wheel_name = "apache_airflow-1.0-py3-none-any.whl"
        before = (destination / wheel_name).read_bytes()
        with zipfile.ZipFile(context / wheel_name) as archive:
            contents = {name: archive.read(name) for name in archive.namelist()}
        path = f"apache_airflow-1.0.dist-info/{metadata_file}"
        contents[path] += b"Requires-Dist: new-dependency\n" if metadata_file == "METADATA" else b"Build: 2\n"
        with zipfile.ZipFile(context / wheel_name, "w") as archive:
            for name, content in contents.items():
                archive.writestr(name, content)
        prepare_dependency_context(context, "3.10")
        assert (destination / wheel_name).read_bytes() != before

    def test_preserves_config_constraints_and_other_distributions(self, context: Path):
        (context / "pip.conf").write_text("[global]\n")
        constraints = context / "constraints-3.10"
        constraints.mkdir()
        (constraints / "constraints-3.10.txt").write_text("example-dependency==1.0\n")
        other_wheel = context / "example_dependency-1.0-py3-none-any.whl"
        other_wheel.write_bytes(b"non-Airflow distribution copied verbatim")
        destination = prepare_dependency_context(context, "3.10")
        assert (destination / "pip.conf").read_bytes() == (context / "pip.conf").read_bytes()
        assert (
            destination / "constraints-3.10/constraints-3.10.txt"
        ).read_text() == "example-dependency==1.0\n"
        assert (destination / other_wheel.name).read_bytes() == other_wheel.read_bytes()
        assert (destination / "metadata-distributions.txt").read_text() == "apache-airflow\n"

    def test_removed_inputs_are_not_retained(self, context: Path):
        (context / "requirements.txt").write_text("obsolete-package\n")
        destination = prepare_dependency_context(context, "3.10")
        (context / "requirements.txt").unlink()
        prepare_dependency_context(context, "3.10")
        assert not (destination / "requirements.txt").exists()
        assert not (destination / ".dependency-cache").exists()

    def test_constraint_timestamps_do_not_invalidate_dependencies(self, context: Path):
        constraints_dir = context / "constraints-3.10"
        constraints_dir.mkdir()
        constraints = constraints_dir / "constraints-source-providers-3.10.txt"
        requirements = 'example-dependency==1.0; python_version >= "3.10"\n'
        constraints.write_text("# Generated on first run\n" + requirements)
        destination = prepare_dependency_context(context, "3.10")
        normalized = destination / "constraints-3.10" / constraints.name
        before = normalized.read_bytes()
        constraints.write_text("  # Generated on second run\n" + requirements)
        prepare_dependency_context(context, "3.10")
        assert normalized.read_bytes() == before == requirements.encode()
        assert constraints.read_text() == "  # Generated on second run\n" + requirements
        constraints.write_text("# Same comment\nexample-dependency==2.0\n")
        prepare_dependency_context(context, "3.10")
        assert normalized.read_bytes() != before

    @pytest.mark.parametrize("extension", ["tar.gz", "zip"])
    def test_source_distributions_use_regular_install(self, context: Path, extension: str):
        (context / f"example-1.0.{extension}").touch()
        assert prepare_dependency_context(context, "3.10") is None

    def test_no_local_airflow_wheels_use_regular_install(self, tmp_path: Path):
        assert prepare_dependency_context(tmp_path, "3.10") is None

    def test_invalid_wheel_metadata_fails(self, tmp_path: Path):
        source = tmp_path / "invalid.whl"
        with zipfile.ZipFile(source, "w") as archive:
            archive.writestr("code.py", "")
        with pytest.raises(ValueError, match="Expected one METADATA"):
            write_metadata_wheel(source, tmp_path / "out.whl")


class TestProductionCacheArguments:
    def test_cleanup_removes_metadata_context(self, context: Path, monkeypatch: pytest.MonkeyPatch):
        (context / ".README.md").write_text("keep")
        prepare_dependency_context(context, "3.10")
        monkeypatch.setattr("airflow_breeze.commands.production_image_commands.DOCKER_CONTEXT_PATH", context)
        clean_docker_context_files()
        assert [path.name for path in context.iterdir()] == [".README.md"]

    @mock.patch(
        "airflow_breeze.utils.docker_command_utils.check_if_buildx_plugin_installed", return_value=False
    )
    def test_cache_export_includes_dependency_layers(self, _buildx, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PROD_IMAGE_BUILD_CACHE_FROM", "/cache/in")
        monkeypatch.setenv("PROD_IMAGE_BUILD_CACHE_TO", "/cache/out")
        command = prepare_docker_build_command(BuildProdParams(dependency_context="metadata"))
        assert "--cache-from=type=local,src=/cache/in" in command
        assert "--cache-to=type=local,dest=/cache/out,mode=max" in command
        assert "DOCKER_CONTEXT_DEPENDENCY_FILES=metadata" in command
        assert "INSTALL_CONTEXT_DEPENDENCIES_ONLY=true" in command

    @mock.patch(
        "airflow_breeze.utils.docker_command_utils.check_if_buildx_plugin_installed", return_value=False
    )
    def test_prod_cache_options_do_not_affect_ci(self, _buildx, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("PROD_IMAGE_BUILD_CACHE_TO", "/cache/out")
        command = prepare_docker_build_command(BuildCiParams())
        assert not any("/cache/out" in item for item in command)
