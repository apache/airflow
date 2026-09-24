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
"""Tests for ZipImporter."""

from __future__ import annotations

import os
import py_compile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.importers import (
    AbstractDagImporter,
    DagDefinition,
    DagImportError,
    DagImportResult,
    DagSourceCode,
    FilesystemDagDefinition,
    ZipImporter,
    ZipMemberDagDefinition,
)
from airflow.sdk.importers.python_importer import PythonDagImporter

if TYPE_CHECKING:
    from airflow.sdk import DAG


class CustomInternalNonExtensionImporter(AbstractDagImporter):
    """An internal importer that routes archive members by filename pattern."""

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        path_str = str(getattr(definition, "file_path", definition))
        return "workflow" in path_str

    def list_dag_definitions(self, bundle, **kwargs):
        return iter([])

    def import_definition(self, definition, bundle=None, **kwargs):
        from airflow.sdk import DAG

        return DagImportResult(dags=[DAG("workflow_dag")])

    def get_source_code(self, definition):
        return DagSourceCode(source_code="", language="text")


def _import_all(
    importer: AbstractDagImporter[DagDefinition],
    bundle,
) -> tuple[list[DAG], list[DagImportError]]:
    """Enumerate an importer's definitions and import each, aggregating dags/errors."""
    dags, errors = [], []
    for item in importer.list_dag_definitions(bundle):
        match item:
            case DagImportError():
                errors.append(item)
            case DagDefinition():
                result = importer.import_definition(item, bundle=bundle)
                dags.extend(result.dags)
                errors.extend(result.errors)
            case _:
                raise ValueError(f"unrecognized dag definition {item!r}")
    return dags, errors


class TestZipImporter:
    """Test the ZipImporter composite implementation."""

    @pytest.fixture
    def mock_bundle(self, tmp_path):
        return SimpleNamespace(name="test_bundle", path=tmp_path)

    def test_list_dag_definitions(self, mock_bundle):
        zip_path = mock_bundle.path / "sample.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.py", "from airflow.sdk import DAG\n")
        (mock_bundle.path / "corrupt.zip").write_bytes(b"not a valid zip and no dag markers")

        # The valid archive yields its member; the unreadable one is surfaced in-band as a
        # DagImportError rather than dropped.
        items = list(ZipImporter().list_dag_definitions(mock_bundle))
        members = [i for i in items if not isinstance(i, DagImportError)]
        errors = [i for i in items if isinstance(i, DagImportError)]
        assert [(d.zip_path, d.file_path) for d in members] == [(zip_path, "dag.py")]
        assert [e.error_type for e in errors] == ["zip_read_error"]

    def test_list_prefers_source_over_pyc_and_skips_pycache(self, mock_bundle):
        zip_path = mock_bundle.path / "compiled.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("foo.py", "from airflow.sdk import DAG\n")
            z.writestr("foo.pyc", b"compiled")  # side-by-side -> skipped
            z.writestr("bar.pyc", b"airflow dag")  # sourceless (has markers) -> kept
            z.writestr("__pycache__/foo.cpython-311.pyc", b"compiled")  # cache -> skipped

        definitions = list(ZipImporter().list_dag_definitions(mock_bundle))
        assert sorted(d.file_path for d in definitions) == ["bar.pyc", "foo.py"]

    def test_list_dedups_over_supported_candidates_only(self, mock_bundle):
        # An unsupported .py must not suppress the supported .pyc beside it.
        zip_path = mock_bundle.path / "bytecode_only.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.py", "from airflow.sdk import DAG\n")
            z.writestr("dag.pyc", b"airflow dag")

        importer = ZipImporter(internal_importers={".pyc": PythonDagImporter()})
        definitions = list(importer.list_dag_definitions(mock_bundle))
        assert [d.file_path for d in definitions] == ["dag.pyc"]

    def test_list_pairs_source_and_bytecode_case_insensitively(self, mock_bundle):
        # `dag.PY` and `dag.pyc` are the same module, so only the source survives.
        zip_path = mock_bundle.path / "mixed_case.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.PY", "from airflow.sdk import DAG\n")
            z.writestr("dag.pyc", b"airflow dag")

        definitions = list(ZipImporter().list_dag_definitions(mock_bundle))
        assert [d.file_path for d in definitions] == ["dag.PY"]

    def test_list_continues_past_unreadable_member(self, mock_bundle):
        # A single bad member is reported and the rest of the archive is still discovered.
        zip_path = mock_bundle.path / "bad_member.zip"
        payload = b"# airflow dag BBBB\n"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as z:
            z.writestr("a_dag.py", "from airflow.sdk import DAG\n")
            z.writestr("b_bad.py", payload)
            z.writestr("c_dag.py", "from airflow.sdk import DAG\n")
        # Rewrite the stored bytes in place so the recorded CRC no longer matches.
        zip_path.write_bytes(zip_path.read_bytes().replace(payload, b"# airflow dag CCCC\n"))

        items = list(ZipImporter().list_dag_definitions(mock_bundle))
        members = [i for i in items if not isinstance(i, DagImportError)]
        errors = [i for i in items if isinstance(i, DagImportError)]
        assert [d.file_path for d in members] == ["a_dag.py", "c_dag.py"]
        assert [e.error_type for e in errors] == ["zip_read_error"]
        assert errors[0].source_reference == os.path.join("bad_member.zip", "b_bad.py")

    def test_import_zip_archive_with_dags(self, mock_bundle):
        zip_path = mock_bundle.path / "sample_dags.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag_a.py", "from airflow.sdk import DAG\ndag = DAG('zip_dag_a')\n")
            z.writestr("helper.py", "def util(): return 1\n")

        dags, errors = _import_all(ZipImporter(), mock_bundle)
        assert len(dags) == 1
        assert dags[0].dag_id == "zip_dag_a"
        assert dags[0].bundle_name == "test_bundle"
        assert len(errors) == 0

    def test_import_zip_archive_with_pyc_dag(self, mock_bundle, tmp_path):
        source_file = tmp_path / "compiled_dag.py"
        source_file.write_text("from airflow.sdk import DAG\ndag = DAG('zip_pyc_dag')\n")
        pyc_file = tmp_path / "compiled_dag.pyc"
        py_compile.compile(str(source_file), cfile=str(pyc_file))

        zip_path = mock_bundle.path / "sample_pyc_dags.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.write(pyc_file, arcname="compiled_dag.pyc")

        importer = ZipImporter()
        dags, errors = _import_all(importer, mock_bundle)
        assert len(dags) == 1
        assert dags[0].dag_id == "zip_pyc_dag"
        assert len(errors) == 0

        src = importer.get_source_code(
            ZipMemberDagDefinition(zip_path=zip_path, file_path="compiled_dag.pyc")
        )
        assert src.language == "python"
        assert "Sourceless bytecode" in src.source_code

    def test_zipslip_traversal_and_metadata_skipped(self, mock_bundle):
        zip_path = mock_bundle.path / "malicious.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("subfolder/", "")
            z.writestr("__MACOSX/._dag.py", "apple double metadata")
            z.writestr("../evil_dag.py", "from airflow.sdk import DAG\ndag = DAG('evil_dag')\n")
            z.writestr("valid_dag.py", "from airflow.sdk import DAG\ndag = DAG('valid_dag')\n")

        dags, _ = _import_all(ZipImporter(), mock_bundle)
        assert len(dags) == 1
        assert dags[0].dag_id == "valid_dag"
        assert not (mock_bundle.path.parent / "evil_dag.py").exists()

    def test_corrupted_zip_file(self, mock_bundle):
        bad_zip = mock_bundle.path / "corrupted.zip"
        bad_zip.write_bytes(b"not a real zip")

        # An unreadable archive is surfaced in-band as a DagImportError, not silently dropped.
        items = list(ZipImporter().list_dag_definitions(mock_bundle))
        assert len(items) == 1
        assert isinstance(items[0], DagImportError)
        assert items[0].error_type == "zip_read_error"

    def test_zip_member_cross_import_via_python_importer(self, mock_bundle):
        # A member imported directly through PythonDagImporter (as the registry routes it by
        # suffix, not via ZipImporter) still resolves sibling-member imports, because the
        # definition's import_context puts its archive on sys.path.
        zip_path = mock_bundle.path / "cross.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("helper_mod.py", "VALUE = 7\n")
            z.writestr(
                "main_dag.py",
                "from airflow.sdk import DAG\nimport helper_mod\ndag = DAG(f'cross_{helper_mod.VALUE}')\n",
            )

        member = ZipMemberDagDefinition(zip_path=zip_path, file_path="main_dag.py")
        result = PythonDagImporter().import_definition(member, bundle=mock_bundle)

        assert result.errors == []
        assert [d.dag_id for d in result.dags] == ["cross_7"]

    def test_zip_member_fileloc_uses_os_sep(self, tmp_path):
        # The fileloc/relative loc join with os.sep (archive.zip/member.py) -- the form
        # airflow-core's ZIP_REGEX and open_maybe_zipped understand -- not a colon.
        member = ZipMemberDagDefinition(zip_path=tmp_path / "a.zip", file_path="sub/dag.py")
        assert repr(member) == os.path.join(str(tmp_path / "a.zip"), "sub/dag.py")
        assert member.get_relative_loc(tmp_path) == os.path.join("a.zip", "sub/dag.py")

    def test_get_source_code_reads_member_not_archive(self, tmp_path):
        zip_path = tmp_path / "source_dags.zip"
        dag_content = "from airflow.sdk import DAG\ndag = DAG('src_dag')\n"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("my_dag.py", dag_content)

        importer = ZipImporter()

        # A zip is a directory of DAG files: each member is its own source unit,
        # rendered through its file-type importer (same single-member semantics as
        # the legacy code view's open_maybe_zipped).
        src_member = importer.get_source_code(
            ZipMemberDagDefinition(zip_path=zip_path, file_path="my_dag.py")
        )
        assert src_member.language == "python"
        assert src_member.source_code == dag_content

        # The archive as a whole has no source, the same way a directory does not.
        with pytest.raises(ValueError, match="No internal importer"):
            importer.get_source_code(FilesystemDagDefinition(path=zip_path))

    def test_zip_dag_definition_freshness_token(self, tmp_path):
        zip_path = tmp_path / "fresh_bundle.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.py", "from airflow.sdk import DAG\n")

        member_def = ZipMemberDagDefinition(zip_path=zip_path, file_path="dag.py")
        stat = zip_path.stat()
        assert member_def.freshness_token == f"{stat.st_mtime_ns}-{stat.st_size}-dag.py"

    def test_zip_importer_internal_importers_from_list_of_specs(self, tmp_path, mock_bundle):
        importer = ZipImporter(
            internal_importers=[
                {
                    "classpath": "airflow.sdk.importers.python_importer.PythonDagImporter",
                    "extensions": [".custom_py"],
                }
            ]
        )
        assert ".custom_py" in importer._internal_extension_importers
        assert ".py" not in importer._internal_extension_importers

        zip_path = mock_bundle.path / "custom_dags.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.custom_py", "from airflow.sdk import DAG\ndag = DAG('custom_zip_dag')\n")

        dags, _ = _import_all(importer, mock_bundle)
        assert len(dags) == 1
        assert dags[0].dag_id == "custom_zip_dag"

    def test_zip_importer_internal_importers_from_dict(self):
        importer = ZipImporter(
            internal_importers={
                ".py": PythonDagImporter(),
                ".alt": {
                    "classpath": "airflow.sdk.importers.python_importer.PythonDagImporter",
                    "extensions": [".alt"],
                },
            }
        )
        assert ".py" in importer._internal_extension_importers
        assert ".alt" in importer._internal_extension_importers

    @pytest.mark.parametrize(
        ("config", "match"),
        [
            ("not_list_or_dict", "must be a list or dictionary"),
            ([{"extensions": [".py"]}], "Missing required 'classpath'"),
            ([{"classpath": "invalid.path"}], "Failed to load DAG importer"),
            (
                [{"classpath": "builtins.dict"}],
                r"must inherit from AbstractDagImporter",
            ),
            ({".py": {"kwargs": {}}}, "Missing required 'classpath'"),
            ({".py": "invalid"}, "expected AbstractDagImporter or dictionary"),
        ],
    )
    def test_zip_importer_invalid_configurations(self, config, match):
        with pytest.raises(AirflowConfigException, match=match):
            ZipImporter(internal_importers=config)

    def test_zip_importer_custom_extensions(self):
        importer = ZipImporter(extensions=[".bundle", ".zip"])
        assert importer.can_handle("test.bundle")
        assert importer.can_handle("test.zip")
        assert set(importer.supported_extensions) == {".bundle", ".zip"}

    def test_zip_importer_internal_importers_non_extension(self, mock_bundle):
        """ZipImporter can route archive members to non-extension internal importers."""
        importer = ZipImporter(
            internal_importers=[
                {
                    "classpath": f"{__name__}.CustomInternalNonExtensionImporter",
                }
            ]
        )
        zip_path = mock_bundle.path / "workflow_archive.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("my_workflow_file", "steps:\n  - run: echo hello\n")

        dags, _ = _import_all(importer, mock_bundle)
        assert len(dags) == 1
        assert dags[0].dag_id == "workflow_dag"
