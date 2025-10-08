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


class DeprecatedImportWarning(FutureWarning):
    """
    Warning class for deprecated imports in Airflow.

    This warning is raised when users import deprecated classes or functions
    from Airflow modules that have been moved to better locations.
    """

    ...


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
    :param override_deprecated_classes: override target attributes with deprecated ones. If target attribute is
       found in the dictionary, it will be displayed in the warning message.
    :param extra_message: extra message to display in the warning or import error message
    :param name: attribute name
    :return:
    """
    target_class_full_name = imports.get(name)

    # Handle wildcard pattern "*" - redirect all attributes to target module
    # Skip Python special attributes (dunder attributes) as they shouldn't be redirected
    if not target_class_full_name and "*" in imports and not (name.startswith("__") and name.endswith("__")):
        target_class_full_name = f"{imports['*']}.{name}"

    if not target_class_full_name:
        raise AttributeError(f"The module `{module!r}` has no attribute `{name!r}`")

    # Determine the warning class name (may be overridden)
    warning_class_name = target_class_full_name
    if override_deprecated_classes and name in override_deprecated_classes:
        warning_class_name = override_deprecated_classes[name]

    message = f"The `{module}.{name}` attribute is deprecated. Please use `{warning_class_name!r}`."
    if extra_message:
        message += f" {extra_message}."
    warnings.warn(message, DeprecatedImportWarning, stacklevel=2)

    # Import and return the target attribute
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
    Add deprecated attribute PEP-563 imports and warnings modules to the package.

    Works for classes, functions, variables, and other module attributes.
    Supports both creating virtual modules and modifying existing modules.

    :param module_imports: imports to use. Format: dict[str, dict[str, str]]
        - Keys are module names (creates virtual modules)
        - Special key __name__ modifies the current module for direct attribute imports
        - Can mix both approaches in a single call
    :param package: package name (typically __name__)
    :param override_deprecated_classes: override target attributes with deprecated ones.
        Format: dict[str, dict[str, str]] matching the structure of module_imports
    :param extra_message: extra message to display in the warning or import error message

    Examples:
        # Create virtual modules (e.g., for removed .py files)
        add_deprecated_classes(
            {"basenotifier": {"BaseNotifier": "airflow.sdk.bases.notifier.BaseNotifier"}},
            package=__name__,
        )

        # Wildcard support - redirect all attributes to new module
        add_deprecated_classes(
            {"timezone": {"*": "airflow.sdk.timezone"}},
            package=__name__,
        )

        # Current module direct imports
        add_deprecated_classes(
            {
                __name__: {
                    "get_fs": "airflow.sdk.io.fs.get_fs",
                    "has_fs": "airflow.sdk.io.fs.has_fs",
                }
            },
            package=__name__,
        )

        # Mixed behavior - both current module and submodule attributes
        add_deprecated_classes(
            {
                __name__: {
                    "get_fs": "airflow.sdk.io.fs.get_fs",
                    "has_fs": "airflow.sdk.io.fs.has_fs",
                    "Properties": "airflow.sdk.io.typedef.Properties",
                },
                "typedef": {
                    "Properties": "airflow.sdk.io.typedef.Properties",
                }
            },
            package=__name__,
        )

    The first example makes 'from airflow.notifications.basenotifier import BaseNotifier' work
    even if 'basenotifier.py' was removed.

    The second example makes 'from airflow.utils.timezone import utc' redirect to 'airflow.sdk.timezone.utc',
    allowing any attribute from the deprecated module to be accessed from the new location.

    The third example makes 'from airflow.io import get_fs' work with direct imports from the current module.

    The fourth example handles both direct imports from the current module and submodule imports.
    """
    # Handle both current module and virtual module deprecations
    for module_name, imports in module_imports.items():
        if module_name == package:
            # Special case: modify the current module for direct attribute imports
            if package not in sys.modules:
                raise ValueError(f"Module {package} not found in sys.modules")

            module = sys.modules[package]

            # Create the __getattr__ function for current module
            current_override = {}
            if override_deprecated_classes and package in override_deprecated_classes:
                current_override = override_deprecated_classes[package]

            getattr_func = functools.partial(
                getattr_with_deprecation,
                imports,
                package,
                current_override,
                extra_message or "",
            )

            # Set the __getattr__ function on the current module
            setattr(module, "__getattr__", getattr_func)
        else:
            # Create virtual modules for submodule imports
            full_module_name = f"{package}.{module_name}"
            module_type = ModuleType(full_module_name)
            if override_deprecated_classes and module_name in override_deprecated_classes:
                override_deprecated_classes_for_module = override_deprecated_classes[module_name]
            else:
                override_deprecated_classes_for_module = {}

            # Mypy is not able to derive the right function signature https://github.com/python/mypy/issues/2427
            module_type.__getattr__ = functools.partial(  # type: ignore[method-assign]
                getattr_with_deprecation,
                imports,
                full_module_name,
                override_deprecated_classes_for_module,
                extra_message or "",
            )
            sys.modules.setdefault(full_module_name, module_type)
