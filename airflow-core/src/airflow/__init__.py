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

# We do not use "from __future__ import annotations" here because it is not supported
# by Pycharm when we want to make sure all imports in airflow work from namespace packages
# Adding it automatically is excluded in pyproject.toml via I002 ruff rule exclusion

# Make `airflow` a namespace package, supporting installing
# airflow.providers.* in different locations (i.e. one in site, and one in user
# lib.)  This is required by some IDEs to resolve the import paths.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

__version__ = "3.2.0"


import os
import sys
import warnings
from typing import TYPE_CHECKING

from airflow.utils.deprecation_tools import DeprecatedImportWarning

if os.environ.get("_AIRFLOW_PATCH_GEVENT"):
    # If you are using gevents and start airflow webserver, you might want to run gevent monkeypatching
    # as one of the first thing when Airflow is started. This allows gevent to patch networking and other
    # system libraries to make them gevent-compatible before anything else patches them (for example boto)
    from gevent.monkey import patch_all

    patch_all()

if sys.platform == "win32":
    warnings.warn(
        "Airflow currently can be run on POSIX-compliant Operating Systems. For development, "
        "it is regularly tested on fairly modern Linux Distros and recent versions of macOS. "
        "On Windows you can run it via WSL2 (Windows Subsystem for Linux 2) or via Linux Containers. "
        "The work to add Windows support is tracked via https://github.com/apache/airflow/issues/10388, "
        "but it is not a high priority.",
        category=RuntimeWarning,
        stacklevel=1,
    )

# The configuration module initializes and validates the conf object as a side effect the first
# time it is imported. If it is not imported before importing the settings module, the conf
# object will then be initted/validated as a side effect of it being imported in settings,
# however this can cause issues since those modules are very tightly coupled and can
# very easily cause import cycles in the conf init/validate code (since downstream code from
# those functions likely import settings).
# configuration is therefore initted early here, simply by importing it.
from airflow import configuration, settings

__all__ = [
    "__version__",
    "DAG",
    "Asset",
    "XComArg",
    # TODO: Remove this module in Airflow 3.2
    "Dataset",
]

# Perform side-effects unless someone has explicitly opted out before import
# WARNING: DO NOT USE THIS UNLESS YOU REALLY KNOW WHAT YOU'RE DOING.
# This environment variable prevents proper initialization, and things like
# configs, logging, the ORM, etc. will be broken. It is only useful if you only
# access certain trivial constants and free functions (e.g. `__version__`).
if not os.environ.get("_AIRFLOW__AS_LIBRARY", None):
    settings.initialize()

# Things to lazy import in form {local_name: ('target_module', 'target_name', 'deprecated')}
__lazy_imports: dict[str, tuple[str, str, bool]] = {
    "DAG": (".sdk", "DAG", False),
    "Asset": (".sdk", "Asset", False),
    "XComArg": (".models.xcom_arg", "XComArg", False),
    "version": (".version", "", False),
    # Deprecated lazy imports
    "AirflowException": (".exceptions", "AirflowException", True),
    "Dataset": (".sdk", "Asset", True),
    "Stats": (".observability.stats", "Stats", True),
    "Trace": (".observability.trace", "Trace", True),
    "metrics": (".observability.metrics", "", True),
    "traces": (".observability.traces", "", True),
}
if TYPE_CHECKING:
    # These objects are imported by PEP-562, however, static analyzers and IDE's
    # have no idea about typing of these objects.
    # Add it under TYPE_CHECKING block should help with it.
    from airflow.sdk import DAG, Asset, Asset as Dataset, XComArg


def __getattr__(name: str):
    # PEP-562: Lazy loaded attributes on python modules
    module_path, attr_name, deprecated = __lazy_imports.get(name, ("", "", False))
    if not module_path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if deprecated:
        warnings.warn(
            f"Import {name!r} directly from the airflow module is deprecated and "
            f"will be removed in the future. Please import it from 'airflow{module_path}.{attr_name}'.",
            DeprecatedImportWarning,
            stacklevel=2,
        )

    import importlib

    mod = importlib.import_module(module_path, __name__)
    if attr_name:
        val = getattr(mod, attr_name)
    else:
        val = mod

    # Store for next time
    globals()[name] = val
    return val


if not settings.LAZY_LOAD_PROVIDERS:
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    manager.initialize_providers_list()
    manager.initialize_providers_hooks()
    manager.initialize_providers_extra_links()
if not settings.LAZY_LOAD_PLUGINS:
    from airflow import plugins_manager

    plugins_manager.ensure_plugins_loaded()
