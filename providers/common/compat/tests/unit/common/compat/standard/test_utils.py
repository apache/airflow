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

import builtins

import pytest

from airflow.providers.common.compat.standard import utils

EXPECTED_EXPORTS = (
    "SkipMixin",
    "XCOM_SKIPMIXIN_FOLLOWED",
    "XCOM_SKIPMIXIN_KEY",
    "XCOM_SKIPMIXIN_SKIPPED",
    "prepare_virtualenv",
    "write_python_script",
)


def test_public_exports():
    assert set(utils.__all__) == set(EXPECTED_EXPORTS)


@pytest.mark.parametrize("name", EXPECTED_EXPORTS)
def test_all_compat_imports_work(name):
    """Test that all items in _IMPORT_MAP can be successfully imported."""
    obj = getattr(utils, name)
    assert obj is not None


def test_fallback_when_primary_path_missing(monkeypatch):
    """Test that fallback path is used when the first import path raises ModuleNotFoundError."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "airflow.sdk.bases.skipmixin":
            raise ModuleNotFoundError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert getattr(utils, "SkipMixin") is not None


def test_invalid_import_raises_attribute_error():
    """Test that accessing non-existent attribute raises AttributeError."""
    with pytest.raises(AttributeError, match="has no attribute 'NonExistentUtility'"):
        _ = utils.NonExistentUtility
