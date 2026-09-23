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
"""Tests for PythonDagImporter."""

from __future__ import annotations

import contextlib
import importlib.util
import logging
import marshal
import py_compile
import signal
import sys
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.importers import (
    FileDagDefinition,
    FilesystemDagDefinition,
    PythonDagImporter,
    ZipMemberDagDefinition,
)
from airflow.sdk.importers.python_importer import _DefinitionSourceLoader


@pytest.fixture
def mock_bundle(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(name="test_bundle", path=bundle_dir)


class _InMemoryDagDefinition(FileDagDefinition):
    """A file-like definition backed purely by in-memory bytes (neither a file nor a zip member)."""

    def __init__(self, name: str, source: bytes) -> None:
        self._name = name
        self._source = source

    @property
    def suffix(self) -> str:
        return Path(self._name).suffix.lower()

    @property
    def freshness_token(self) -> str:
        return str(len(self._source))

    def get_relative_loc(self, root: Path | None = None) -> str:
        return self._name

    def read_bytes(self) -> bytes:
        return self._source

    @contextlib.contextmanager
    def as_file(self):
        with tempfile.NamedTemporaryFile(suffix=self.suffix, delete=False) as f:
            f.write(self._source)
            tmp = Path(f.name)
        try:
            yield tmp
        finally:
            tmp.unlink(missing_ok=True)

    def __repr__(self) -> str:
        return f"<memory:{self._name}>"


class TestPythonDagImporter:
    """Test the PythonDagImporter implementation."""

    def test_import_successful_dag(self, mock_bundle):
        dag_file = mock_bundle.path / "sample_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\ndag = DAG('test_dag_1')\n")

        importer = PythonDagImporter()
        definition = FilesystemDagDefinition(path=dag_file)
        result = importer.import_definition(definition, bundle=mock_bundle)

        assert len(result.dags) == 1
        assert result.dags[0].dag_id == "test_dag_1"
        assert result.dags[0].bundle_name == "test_bundle"
        assert result.dags[0].relative_fileloc == "sample_dag.py"
        assert len(result.errors) == 0

    def test_import_syntax_error_cleans_sys_modules(self, mock_bundle):
        dag_file = mock_bundle.path / "bad_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\ndef broken(\n")

        importer = PythonDagImporter()
        result = importer.import_definition(FilesystemDagDefinition(path=dag_file), bundle=mock_bundle)

        assert len(result.errors) == 1
        assert result.errors[0].error_type == "import"
        assert not any("bad_dag" in m for m in sys.modules)

    def test_import_non_dag_file_yields_no_dags(self, mock_bundle):
        # Import does not re-sniff -- discovery already filters non-DAG files. Importing one
        # directly just yields no DAGs (and no error); it is not reported as skipped.
        helper_file = mock_bundle.path / "helper.py"
        helper_file.write_text("def util(): return 42\n")

        importer = PythonDagImporter()
        definition = FilesystemDagDefinition(path=helper_file)
        result = importer.import_definition(definition, bundle=mock_bundle)

        assert result.dags == []
        assert result.errors == []
        assert result.skipped_definitions == []

    def test_import_non_dag_zip_member_yields_no_dags(self, mock_bundle):
        # Same for a zip member imported directly: no sniff at import, so no DAGs and no skip.
        zip_path = mock_bundle.path / "helpers.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("helper.py", "def util():\n    return 42\n")

        importer = PythonDagImporter()
        definition = ZipMemberDagDefinition(zip_path=zip_path, file_path="helper.py")
        result = importer.import_definition(definition, bundle=mock_bundle)

        assert result.dags == []
        assert result.errors == []
        assert result.skipped_definitions == []

    def test_import_corrupt_pyc_captured_as_error(self, mock_bundle):
        # A .pyc whose header is not valid CPython bytecode must surface as an import
        # error (from _DefinitionBytecodeLoader's magic check), not crash the importer.
        bad_pyc = mock_bundle.path / "broken.pyc"
        bad_pyc.write_bytes(b"this is not valid python bytecode at all!!")

        importer = PythonDagImporter()
        definition = FilesystemDagDefinition(path=bad_pyc)
        result = importer.import_definition(definition, bundle=mock_bundle)

        assert len(result.dags) == 0
        assert len(result.errors) == 1
        assert result.errors[0].error_type == "import"

    def test_source_loader_get_data_rejects_foreign_path(self):
        # get_data serves the module's own source, but a sibling-resource request must fail
        # loud rather than return the DAG source for it.
        class _Def:
            def read_bytes(self) -> bytes:
                return b"SOURCE = 1"

            def __repr__(self) -> str:
                return "mydef"

        loader = _DefinitionSourceLoader(_Def())
        assert loader.get_data(loader.get_filename("mydef")) == b"SOURCE = 1"
        with pytest.raises(FileNotFoundError):
            loader.get_data("some/other/config.json")

    def test_import_pyc_with_non_code_payload_captured_as_error(self, mock_bundle):
        # A .pyc with a valid magic header but a payload that unmarshals to a non-code
        # object must be rejected, not exec()'d as source.
        header = importlib.util.MAGIC_NUMBER + b"\x00" * 12
        bad_pyc = mock_bundle.path / "not_code.pyc"
        bad_pyc.write_bytes(header + marshal.dumps("i am a string, not a code object"))

        importer = PythonDagImporter()
        definition = FilesystemDagDefinition(path=bad_pyc)
        result = importer.import_definition(definition, bundle=mock_bundle)

        assert len(result.dags) == 0
        assert len(result.errors) == 1
        assert result.errors[0].error_type == "import"

    def test_imports_arbitrary_file_like_definition(self, mock_bundle):
        # The importer must handle any FileDagDefinition through the interface, with no
        # knowledge of File/Zip concrete types -- so a third, in-memory backing works too.
        source = b"from airflow.sdk import DAG\ndag = DAG('in_memory_dag')\n"
        definition = _InMemoryDagDefinition("in_memory_dag.py", source)

        result = PythonDagImporter().import_definition(definition, bundle=mock_bundle)

        assert [d.dag_id for d in result.dags] == ["in_memory_dag"]
        assert result.errors == []

    def test_list_dag_definitions(self, mock_bundle):
        # Discovery applies the lightweight sniff, so a .py with no DAG markers (helper.py) is
        # filtered out and never becomes a definition; only the real DAG file is returned.
        dag_file = mock_bundle.path / "sample_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\ndag = DAG('test_dag_1')\n")
        (mock_bundle.path / "helper.py").write_text("def helper():\n    return 42\n")
        (mock_bundle.path / "notes.txt").write_text("hello")

        importer = PythonDagImporter()
        defs = list(importer.list_dag_definitions(mock_bundle))
        assert {d.path.name for d in defs} == {"sample_dag.py"}

    def test_list_prefers_source_over_pyc_and_skips_pycache(self, mock_bundle):
        (mock_bundle.path / "foo.py").write_text("from airflow.sdk import DAG\n")
        (mock_bundle.path / "foo.pyc").write_bytes(b"compiled")  # side-by-side -> skipped
        (mock_bundle.path / "bar.pyc").write_bytes(b"airflow dag")  # sourceless (has markers) -> kept
        cache = mock_bundle.path / "__pycache__"
        cache.mkdir()
        (cache / "foo.cpython-311.pyc").write_bytes(b"compiled")  # cache -> skipped

        defs = list(PythonDagImporter().list_dag_definitions(mock_bundle))
        assert sorted(d.path.name for d in defs) == ["bar.pyc", "foo.py"]

    @pytest.mark.parametrize(
        ("filename", "is_bytecode", "expected_content"),
        [
            ("source_dag.py", False, "# My DAG\nfrom airflow.sdk import DAG\n"),
            ("source_dag.pyc", True, "# Sourceless bytecode (.pyc) — source code not available\n"),
        ],
    )
    def test_get_source_code(self, tmp_path, filename, is_bytecode, expected_content):
        dag_file = tmp_path / filename
        if is_bytecode:
            dag_file.write_bytes(b"\x00\x00\x00\x00bytecode")
        else:
            dag_file.write_text(expected_content)

        src = PythonDagImporter().get_source_code(FilesystemDagDefinition(path=dag_file))
        assert src.language == "python"
        assert src.source_code == expected_content

    def test_import_pyc_file(self, mock_bundle, tmp_path):
        source_file = tmp_path / "compiled_dag.py"
        source_file.write_text("from airflow.sdk import DAG\ndag = DAG('compiled_dag')\n")
        pyc_file = mock_bundle.path / "compiled_dag.pyc"
        py_compile.compile(str(source_file), cfile=str(pyc_file))

        importer = PythonDagImporter()
        result = importer.import_definition(FilesystemDagDefinition(path=pyc_file), bundle=mock_bundle)

        assert len(result.dags) == 1
        assert result.dags[0].dag_id == "compiled_dag"
        assert len(result.errors) == 0

    def test_import_sourceless_package_resolves_relative_import(self, mock_bundle, tmp_path):
        # An __init__.pyc must load as a package, otherwise its relative imports fail with
        # "attempted relative import with no known parent package".
        pkg = mock_bundle.path / "sourceless_pkg"
        pkg.mkdir()
        (tmp_path / "helper.py").write_text("ID = 5\n")
        (tmp_path / "__init__.py").write_text(
            "from airflow.sdk import DAG\nfrom .helper import ID\ndag = DAG(f'sourceless_pkg_{ID}')\n"
        )
        for name in ("helper", "__init__"):
            py_compile.compile(str(tmp_path / f"{name}.py"), cfile=str(pkg / f"{name}.pyc"))

        result = PythonDagImporter().import_definition(
            FilesystemDagDefinition(path=pkg / "__init__.pyc"), bundle=mock_bundle
        )

        assert result.errors == []
        assert [d.dag_id for d in result.dags] == ["sourceless_pkg_5"]

    def test_file_dag_definition_freshness_token(self, tmp_path):
        dag_file = tmp_path / "fresh_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\n")
        stat = dag_file.stat()
        assert FilesystemDagDefinition(path=dag_file).freshness_token == f"{stat.st_mtime_ns}-{stat.st_size}"

    def test_python_importer_custom_extensions(self, mock_bundle):
        importer = PythonDagImporter(extensions=[".custom_py"])
        assert importer.can_handle("dag.custom_py")
        assert not importer.can_handle("dag.py")
        assert importer.supported_extensions == [".custom_py"]

        dag_file = mock_bundle.path / "sample_dag.custom_py"
        dag_file.write_text("from airflow.sdk import DAG\ndag = DAG('custom_py_dag')\n")
        defs = list(importer.list_dag_definitions(mock_bundle))
        assert len(defs) == 1
        assert defs[0].path == dag_file

    @pytest.mark.parametrize(
        ("enable_traceback", "expect_traceback"),
        [
            (True, True),
            (False, False),
        ],
    )
    @mock.patch("airflow.sdk.importers.python_importer.conf")
    def test_import_error_traceback_configuration(
        self, mock_conf, enable_traceback, expect_traceback, mock_bundle
    ):
        mock_conf.getboolean.return_value = enable_traceback
        mock_conf.getint.return_value = 2

        dag_file = mock_bundle.path / "bad.py"
        dag_file.write_text("from airflow.sdk import DAG\ndef broken(\n")

        importer = PythonDagImporter()
        result = importer.import_definition(FilesystemDagDefinition(path=dag_file), bundle=mock_bundle)

        assert len(result.errors) == 1
        assert (result.errors[0].stacktrace is not None) == expect_traceback

    def test_invalid_dagbag_import_timeout_raises_custom_exception(self, mock_bundle):
        mock_settings = mock.MagicMock()
        mock_settings.get_dagbag_import_timeout.return_value = "invalid_timeout_str"

        importer = PythonDagImporter()
        with (
            mock.patch.dict("sys.modules", {"airflow": mock.MagicMock(settings=mock_settings)}),
            pytest.raises(
                AirflowConfigException,
                match=r"Value \(invalid_timeout_str\) from get_dagbag_import_timeout must be int or float",
            ),
        ):
            importer.import_definition(
                FilesystemDagDefinition(path=mock_bundle.path / "dag.py"),
                bundle=mock_bundle,
            )

    @mock.patch.object(PythonDagImporter, "_load_modules", side_effect=TypeError("unexpected None"))
    def test_unexpected_type_error_captured_in_result_errors(self, mock_load, mock_bundle):
        importer = PythonDagImporter()
        result = importer.import_definition(
            FilesystemDagDefinition(path=mock_bundle.path / "dag.py"),
            bundle=mock_bundle,
        )

        assert len(result.errors) == 1
        assert result.errors[0].error_type == "import"
        assert "unexpected None" in result.errors[0].message

    def test_sigsegv_handler_registration_and_execution(self, mock_bundle):
        dag_file = mock_bundle.path / "sample_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\ndag = DAG('test_dag')\n")

        importer = PythonDagImporter()
        registered_handler = None

        def mock_signal_func(signum, handler):
            nonlocal registered_handler
            if signum == signal.SIGSEGV:
                registered_handler = handler

        with mock.patch("signal.signal", side_effect=mock_signal_func):
            result = importer.import_definition(FilesystemDagDefinition(path=dag_file), bundle=mock_bundle)
            assert callable(registered_handler)

            registered_handler(signal.SIGSEGV, None)
            assert len(result.errors) == 1
            assert result.errors[0].error_type == "segfault"
            assert "Received SIGSEGV signal while processing" in result.errors[0].message

    def test_sigsegv_handler_registration_failure_logged(self, mock_bundle, caplog):
        dag_file = mock_bundle.path / "sample_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\ndag = DAG('test_dag')\n")

        importer = PythonDagImporter()
        with (
            mock.patch("signal.signal", side_effect=ValueError("signal only works in main thread")),
            caplog.at_level(logging.WARNING),
        ):
            result = importer.import_definition(FilesystemDagDefinition(path=dag_file), bundle=mock_bundle)

        assert len(result.dags) == 1
        assert "SIGSEGV signal handler registration failed. Not in the main thread" in caplog.text
