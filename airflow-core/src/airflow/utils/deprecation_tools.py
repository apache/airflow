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


def getattr_with_deprecation(
    imports: dict[str, str],
    module: str,
    override_deprecated_classes: dict[str, str],
    extra_message: str,
    name: str,
):
    """
    Retrieve the imported attribute from the redirected module and raises a deprecation warning.

    :param imports: dict of imports and their redirection for the module
    :param module: name of the module in the package to get the attribute from
    :param override_deprecated_classes: override target classes with deprecated ones. If target class is
       found in the dictionary, it will be displayed in the warning message.
    :param extra_message: extra message to display in the warning or import error message
    :param name: attribute name
    :return:
    """
    target_class_full_name = imports.get(name)
    if not target_class_full_name:
        raise AttributeError(f"The module `{module!r}` has no attribute `{name!r}`")
    warning_class_name = target_class_full_name
    if override_deprecated_classes and name in override_deprecated_classes:
        warning_class_name = override_deprecated_classes[name]
    message = f"The `{module}.{name}` class is deprecated. Please use `{warning_class_name!r}`."
    if extra_message:
        message += f" {extra_message}."
    warnings.warn(message, DeprecationWarning, stacklevel=2)
    new_module, new_class_name = target_class_full_name.rsplit(".", 1)
    try:
        return getattr(importlib.import_module(new_module), new_class_name)
    except ImportError as e:
        error_message = (
            f"Could not import `{new_module}.{new_class_name}` while trying to import `{module}.{name}`."
        )
        if extra_message:
            error_message += f" {extra_message}."
        raise ImportError(error_message) from e


def add_deprecated_classes(
    module_imports: dict[str, dict[str, str]],
    package: str,
    override_deprecated_classes: dict[str, dict[str, str]] | None = None,
    extra_message: str | None = None,
):
    """
    Add deprecated class PEP-563 imports and warnings modules to the package.

    Side note: It also works for methods, not just classes.

    :param module_imports: imports to use
    :param package: package name
    :param override_deprecated_classes: override target classes with deprecated ones. If module +
       target class is found in the dictionary, it will be displayed in the warning message.
    :param extra_message: extra message to display in the warning or import error message
    """
    for module_name, imports in module_imports.items():
        full_module_name = f"{package}.{module_name}"
        module_type = ModuleType(full_module_name)
        if override_deprecated_classes and module_name in override_deprecated_classes:
            override_deprecated_classes_for_module = override_deprecated_classes[module_name]
        else:
            override_deprecated_classes_for_module = {}

        # Mypy is not able to derive the right function signature https://github.com/python/mypy/issues/2427
        module_type.__getattr__ = functools.partial(  # type: ignore[assignment]
            getattr_with_deprecation,
            imports,
            full_module_name,
            override_deprecated_classes_for_module,
            extra_message or "",
        )
        sys.modules.setdefault(full_module_name, module_type)
