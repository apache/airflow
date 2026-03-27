#!/usr/bin/env python3
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

import pytest


def test_all_compat_imports_work():
    """
    Test that all items in _IMPORT_MAP can be successfully imported.

    For each item, validates that at least one of the specified import paths works,
    ensuring the fallback mechanism is functional.
    """
    from airflow.providers.common.compat import sdk

    failed_imports = []

    for name in sdk.__all__:
        try:
            obj = getattr(sdk, name)
            assert obj is not None, f"{name} imported as None"
        except (ImportError, AttributeError) as e:
            failed_imports.append((name, str(e)))

    if failed_imports:
        error_msg = "The following imports failed:\n"
        for name, error in failed_imports:
            error_msg += f"  - {name}: {error}\n"
        pytest.fail(error_msg)


def test_invalid_import_raises_attribute_error():
    """Test that importing non-existent attribute raises AttributeError."""
    from airflow.providers.common.compat import sdk

    with pytest.raises(AttributeError, match="has no attribute 'NonExistentClass'"):
        _ = sdk.NonExistentClass
