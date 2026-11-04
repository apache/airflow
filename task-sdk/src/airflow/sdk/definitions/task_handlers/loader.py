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
Import the Python modules that register ``@task_handler`` functions.

A non-Python Dag defines a stub task (e.g. YAML's ``run:`` task) but imports no
Python; the ``@task_handler`` bodies live in Python modules in a Dag bundle.
Importing those modules runs the decorators, which register each handler in the
in-memory registry keyed by ``(dag_id, task_id)``. The bundle is either named
(the coordinator's ``task_handler_bundle_name``) or, when unset, the stub's own
Dag bundle.

Resolving a named bundle and importing its modules are separate steps:
:func:`resolve_handler_bundle` turns a name into a pinned bundle, and
:func:`import_handler_modules` imports any already-materialized bundle. Both are
done at most once per process, but Dag processing and task workers are
short-lived, so each process does its own. Every non-ignored Python file under
the bundle is imported, so a handler bundle should not also hold Python Dag files
(see the task-handler documentation).
"""

from __future__ import annotations

import contextlib
import importlib.util
import pathlib
import sys
from typing import TYPE_CHECKING

import structlog

from airflow.sdk._shared.module_loading.dag_file import get_unique_dag_module_name
from airflow.sdk._shared.module_loading.file_discovery import find_path_from_directory

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002

log = structlog.get_logger(logger_name="task_handlers.loader")

_resolved_bundles: dict[str, BaseDagBundle] = {}
_imported_bundles: set[str] = set()


def resolve_handler_bundle(name: str) -> BaseDagBundle:
    """
    Materialize the named handler bundle at a pinned version.

    Resolves *name* through ``DagBundlesManager`` and pins it to a concrete
    version, once per process.
    """
    with contextlib.suppress(KeyError):
        return _resolved_bundles[name]

    from airflow.sdk.api.datamodels._generated import BundleInfo
    from airflow.sdk.execution_time.bundles import initialize_pinned_bundle

    _resolved_bundles[name] = bundle = initialize_pinned_bundle(BundleInfo(name=name), log)
    return bundle


def import_handler_modules(bundle: BaseDagBundle) -> None:
    """
    Import *bundle*'s handler modules so ``@task_handler`` decorators register.

    Imports every non-ignored Python file in *bundle*. The bundle must already
    be materialized. Importing is idempotent per process by bundle name.
    """
    from airflow.dag_processing.bundles.base import BundleVersionLock  # noqa: SDK002

    if bundle.name in _imported_bundles:
        return
    with BundleVersionLock(bundle_name=bundle.name, bundle_version=bundle.version):
        _import_python_files(bundle.path)
    _imported_bundles.add(bundle.name)
    log.debug("Imported task-handler bundle", bundle=bundle.name, path=bundle.path)


def _import_python_files(root: pathlib.Path) -> None:
    """Import every non-ignored ``.py`` under *root*."""
    for found in find_path_from_directory(root, ".airflowignore"):
        path = pathlib.Path(found)
        if path.suffix != ".py" or "__pycache__" in path.parts:
            continue
        if (module_name := get_unique_dag_module_name(found)) in sys.modules:
            continue
        if (spec := importlib.util.spec_from_file_location(module_name, path)) is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        # Register before exec so module-level code that looks itself up in
        # sys.modules works (e.g. @dataclass). Drop it on failure.
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(module_name, None)
            raise
