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

import logging
from unittest import mock

from airflow_shared.module_loading import (
    MODIFIED_DAG_MODULE_NAME,
    UNUSUAL_MODULE_PREFIX,
    get_unique_dag_module_name,
    might_contain_dag,
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
