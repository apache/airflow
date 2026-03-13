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

"""
Reusable utilities for creating compatibility layers with fallback imports.

This module provides the core machinery used by sdk.py and standard/* modules
to handle import fallbacks between Airflow 3.x and 2.x.
"""

from __future__ import annotations

import importlib


def create_module_getattr(
    import_map: dict[str, str | tuple[str, ...]],
    module_map: dict[str, str | tuple[str, ...]] | None = None,
    rename_map: dict[str, tuple[str, str, str]] | None = None,
):
    """
    Create a __getattr__ function for lazy imports with fallback support.

    :param import_map: Dictionary mapping attribute names to module paths (single or tuple for fallback)
    :param module_map: Dictionary mapping module names to module paths (single or tuple for fallback)
    :param rename_map: Dictionary mapping new names to (new_path, old_path, old_name) tuples
    :return: A __getattr__ function that can be assigned at module level
    """
    module_map = module_map or {}
    rename_map = rename_map or {}

    def __getattr__(name: str):
        # Check renamed imports first
        if name in rename_map:
            new_path, old_path, old_name = rename_map[name]

            rename_error: ImportError | ModuleNotFoundError | AttributeError | None = None
            # Try new path with new name first (Airflow 3.x)
            try:
                module = __import__(new_path, fromlist=[name])
                return getattr(module, name)
            except (ImportError, ModuleNotFoundError, AttributeError) as e:
                rename_error = e

            # Fall back to old path with old name (Airflow 2.x)
            try:
                module = __import__(old_path, fromlist=[old_name])
                return getattr(module, old_name)
            except (ImportError, ModuleNotFoundError, AttributeError):
                if rename_error:
                    raise ImportError(
                        f"Could not import {name!r} from {new_path!r} or {old_name!r} from {old_path!r}"
                    ) from rename_error
                raise

        # Check module imports
        if name in module_map:
            value = module_map[name]
            paths = value if isinstance(value, tuple) else (value,)

            module_error: ImportError | ModuleNotFoundError | None = None
            for module_path in paths:
                try:
                    return importlib.import_module(module_path)
                except (ImportError, ModuleNotFoundError) as e:
                    module_error = e
                    continue

            if module_error:
                raise ImportError(f"Could not import module {name!r} from any of: {paths}") from module_error

        # Check regular imports
        if name in import_map:
            value = import_map[name]
            paths = value if isinstance(value, tuple) else (value,)

            attr_error: ImportError | ModuleNotFoundError | AttributeError | None = None
            for module_path in paths:
                try:
                    module = __import__(module_path, fromlist=[name])
                    return getattr(module, name)
                except (ImportError, ModuleNotFoundError, AttributeError) as e:
                    attr_error = e
                    continue

            if attr_error:
                raise ImportError(f"Could not import {name!r} from any of: {paths}") from attr_error

        raise AttributeError(f"module has no attribute {name!r}")

    return __getattr__
