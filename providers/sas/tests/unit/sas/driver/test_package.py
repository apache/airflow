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
"""The driver package stays independent of Airflow, so it can be used, or released, on its own."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import airflow.providers.sas.driver as driver

DRIVER_MODULES = sorted(Path(driver.__file__).parent.glob("*.py"))


def _imported_modules(path: Path) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            imported.add(node.module)
    return imported


@pytest.mark.parametrize("path", DRIVER_MODULES, ids=lambda path: path.name)
def test_driver_module_imports_nothing_from_airflow(path):
    airflow_imports = {
        module
        for module in _imported_modules(path)
        if module.split(".")[0] == "airflow" and not module.startswith("airflow.providers.sas.driver")
    }

    assert not airflow_imports
