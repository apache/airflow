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

import contextlib
import logging
import tempfile
from pathlib import Path
from unittest import mock

from airflow_shared.module_loading import (
    MODIFIED_DAG_MODULE_NAME,
    UNUSUAL_MODULE_PREFIX,
    accepts_dag_definition,
    get_unique_dag_module_name,
    might_contain_dag,
    might_contain_dag_via_default_heuristic,
)


def test_constants() -> None:
    """Test that the constants are as expected."""
    assert UNUSUAL_MODULE_PREFIX == "unusual_prefix_"
    assert MODIFIED_DAG_MODULE_NAME == "unusual_prefix_{path_hash}_{module_name}"


def test_get_unique_dag_module_name() -> None:
    mod_name = get_unique_dag_module_name("/path/to/my_dag.py")
    assert mod_name.startswith("unusual_prefix_")
    assert mod_name.endswith("_my_dag")


def test_might_contain_dag(tmp_path) -> None:
    mock_conf = mock.MagicMock()
    mock_conf.getimport.return_value = None

    dag_file = tmp_path / "test_dag.py"
    dag_file.write_text("from airflow import DAG\ndag = DAG('test')")
    assert might_contain_dag(str(dag_file), safe_mode=True, conf=mock_conf) is True

    non_dag_file = tmp_path / "helper.py"
    non_dag_file.write_text("def add(x, y): return x + y")
    assert might_contain_dag(str(non_dag_file), safe_mode=True, conf=mock_conf) is False
    assert might_contain_dag(str(non_dag_file), safe_mode=False, conf=mock_conf) is True


def test_might_contain_dag_with_explicit_conf() -> None:
    mock_conf = mock.MagicMock()
    mock_conf.getimport.return_value = lambda file_path, zip_file=None: False

    assert might_contain_dag("sample.py", safe_mode=True, conf=mock_conf) is False
    mock_conf.getimport.assert_called_once_with("core", "might_contain_dag_callable", fallback=None)


def test_might_contain_dag_logs_warning_on_broken_config(tmp_path, caplog) -> None:
    dag_file = tmp_path / "test_dag.py"
    dag_file.write_text("from airflow import DAG\ndag = DAG('test')")

    mock_conf = mock.MagicMock()
    mock_conf.getimport.side_effect = ImportError("No module named 'broken_module'")

    with caplog.at_level(logging.WARNING):
        result = might_contain_dag(str(dag_file), safe_mode=True, conf=mock_conf)

    assert result is True
    assert "Failed to load might_contain_dag_callable from config" in caplog.text


class _FakeDefinition:
    """Structural DagDefinition: read its bytes, or materialize it as a file."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def read_bytes(self) -> bytes:
        return self._data

    @contextlib.contextmanager
    def as_file(self):
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
            f.write(self._data)
            path = Path(f.name)
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)


def test_might_contain_dag_from_definition() -> None:
    # The default heuristic reads a definition's bytes directly, with nothing on disk.
    mock_conf = mock.MagicMock()
    mock_conf.getimport.return_value = None

    assert (
        might_contain_dag(
            _FakeDefinition(b"from airflow import DAG\ndag = DAG('x')"), safe_mode=True, conf=mock_conf
        )
        is True
    )
    assert (
        might_contain_dag(_FakeDefinition(b"def add(x, y): return x + y"), safe_mode=True, conf=mock_conf)
        is False
    )
    assert might_contain_dag(_FakeDefinition(b"anything"), safe_mode=False, conf=mock_conf) is True


def test_default_heuristic_accepts_definition() -> None:
    # The default heuristic reads a definition's bytes directly, same as it reads a path.
    assert might_contain_dag_via_default_heuristic(_FakeDefinition(b"from airflow import DAG")) is True
    assert might_contain_dag_via_default_heuristic(_FakeDefinition(b"x = 1")) is False


class _NoFileDefinition(_FakeDefinition):
    """A definition that refuses to materialize, proving the bytes path was taken."""

    def as_file(self):
        raise AssertionError("definition should not be materialized")


def test_configured_default_heuristic_reads_bytes() -> None:
    # The shipped config points might_contain_dag_callable at the default heuristic, so a
    # definition must still be passed straight through rather than written to a temp file.
    mock_conf = mock.MagicMock()
    mock_conf.getimport.return_value = might_contain_dag_via_default_heuristic

    definition = _NoFileDefinition(b"from airflow import DAG")
    assert might_contain_dag(definition, safe_mode=True, conf=mock_conf) is True


def test_marked_custom_callable_receives_definition() -> None:
    # Any callable can opt into taking definitions by marking itself.
    @accepts_dag_definition
    def custom(file_path, zip_file=None):
        return b"airflow" in file_path.read_bytes()

    mock_conf = mock.MagicMock()
    mock_conf.getimport.return_value = custom

    definition = _NoFileDefinition(b"from airflow import DAG")
    assert might_contain_dag(definition, safe_mode=True, conf=mock_conf) is True


def test_might_contain_dag_definition_materialized_for_custom_callable() -> None:
    # A custom callable only understands (file_path, zip_file); a definition is materialized
    # through its own as_file() so the callable gets a real, readable path.
    seen: dict[str, object] = {}

    def custom(file_path, zip_file=None):
        seen["path"] = file_path
        seen["zip_file"] = zip_file
        seen["data"] = Path(file_path).read_bytes()
        return b"airflow" in seen["data"]

    mock_conf = mock.MagicMock()
    mock_conf.getimport.return_value = custom

    assert (
        might_contain_dag(_FakeDefinition(b"from airflow import DAG"), safe_mode=True, conf=mock_conf) is True
    )
    assert seen["data"] == b"from airflow import DAG"
    assert seen["zip_file"] is None
    assert not Path(str(seen["path"])).exists()
