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

import functools
import importlib
import sys
import warnings
from types import ModuleType


def getattr_with_deprecation(imports: dict[str, str], module: str, name: str):
    target_class_full_name = imports.get(name)
    if not target_class_full_name:
        raise AttributeError(f"The module `{module!r}` has no attribute `{name!r}`")
    warnings.warn(
        f"The `{module}.{name}` class is deprecated. Please use `{target_class_full_name!r}`.",
        DeprecationWarning,
        stacklevel=2,
    )
    new_module, new_class_name = target_class_full_name.rsplit(".", 1)
    return getattr(importlib.import_module(new_module), new_class_name)


def add_deprecated_classes(module_imports: dict[str, dict[str, str]], package: str):
    for module_name, imports in module_imports.items():
        full_module_name = f"{package}.{module_name}"
        module_type = ModuleType(full_module_name)
        # Mypy is not able to derive the right function signature https://github.com/python/mypy/issues/2427
        module_type.__getattr__ = functools.partial(  # type: ignore[assignment]
            getattr_with_deprecation, imports, full_module_name
        )
        sys.modules.setdefault(full_module_name, module_type)
