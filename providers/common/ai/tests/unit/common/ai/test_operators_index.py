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
"""Guard against operators going undocumented in docs/operators/index.rst."""

from __future__ import annotations

import inspect
import pkgutil
from importlib import import_module
from pathlib import Path

from airflow.providers.common.ai import operators
from airflow.sdk import BaseOperator

PROVIDER_ROOT = Path(__file__).resolve().parents[4]
INDEX_RST = PROVIDER_ROOT / "docs" / "operators" / "index.rst"


def _iter_operator_classes():
    for module_info in pkgutil.iter_modules(operators.__path__, prefix=f"{operators.__name__}."):
        module = import_module(module_info.name)
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                obj.__module__ == module.__name__
                and issubclass(obj, BaseOperator)
                and obj is not BaseOperator
            ):
                yield name


def test_all_operators_are_listed_in_docs_index():
    index_contents = INDEX_RST.read_text()

    undocumented = sorted(name for name in _iter_operator_classes() if name not in index_contents)

    assert not undocumented, (
        f"Operator(s) {undocumented} are missing from the 'Choosing the right operator' table in {INDEX_RST}"
    )
