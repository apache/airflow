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
"""
Authentication is implemented using flask_login and different environments can
implement their own login mechanisms by providing an `airflow_login` module
in their PYTHONPATH. airflow_login should be based off the
`airflow.www.login`

isort:skip_file
"""
from __future__ import annotations

# flake8: noqa: F401

import os
import sys
from typing import Callable

from airflow import settings

__all__ = ["__version__", "login", "DAG", "PY36", "PY37", "PY38", "PY39", "PY310", "XComArg"]

# Make `airflow` an namespace package, supporting installing
# airflow.providers.* in different locations (i.e. one in site, and one in user
# lib.)
__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

# Perform side-effects unless someone has explicitly opted out before import
# WARNING: DO NOT USE THIS UNLESS YOU REALLY KNOW WHAT YOU'RE DOING.
if not os.environ.get("_AIRFLOW__AS_LIBRARY", None):
    settings.initialize()

login: Callable | None = None

PY36 = sys.version_info >= (3, 6)
PY37 = sys.version_info >= (3, 7)
PY38 = sys.version_info >= (3, 8)
PY39 = sys.version_info >= (3, 9)
PY310 = sys.version_info >= (3, 10)

# Things to lazy import in form {local_name: ('target_module', 'target_name')}
__lazy_imports: dict[str, tuple[str, str]] = {
    "DAG": (".models.dag", "DAG"),
    "Dataset": (".datasets", "Dataset"),
    "XComArg": (".models.xcom_arg", "XComArg"),
    "AirflowException": (".exceptions", "AirflowException"),
    "version": (".version", ""),
    "__version__": (".version", "version"),
}


def __getattr__(name: str):
    # PEP-562: Lazy loaded attributes on python modules
    module_path, attr_name = __lazy_imports.get(name, ("", ""))
    if not module_path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    import importlib

    mod = importlib.import_module(module_path, __name__)
    if attr_name:
        val = getattr(mod, attr_name)
    else:
        val = mod

    # Store for next time
    globals()[name] = val
    return val


if not settings.LAZY_LOAD_PLUGINS:
    from airflow import plugins_manager

    plugins_manager.ensure_plugins_loaded()

if not settings.LAZY_LOAD_PROVIDERS:
    from airflow import providers_manager

    manager = providers_manager.ProvidersManager()
    manager.initialize_providers_list()
    manager.initialize_providers_hooks()
    manager.initialize_providers_extra_links()


# This is never executed, but tricks static analyzers (PyDev, PyCharm,)
# into knowing the types of these symbols, and what
# they contain.
STATICA_HACK = True
globals()["kcah_acitats"[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.dag import DAG
    from airflow.models.xcom_arg import XComArg
    from airflow.exceptions import AirflowException
