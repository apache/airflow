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
Python. The ``@task_handler`` bodies live in a separate module. The Dag bundle
that contains a handler is resolved by name, and its Python files are imported
so the decorators run and populate the in-memory handler registry.

The same code path is used by the parse path for validation, and the execution
path to run the handler.

Import is per-process and once per bundle. A redeployed bundle is therefore only
picked up on the next fresh processing loop.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys

import structlog

from airflow.sdk._shared.module_loading.dag_file import get_unique_dag_module_name

log = structlog.get_logger(logger_name="task_handlers.loader")

_loaded_bundles: set[str] = set()


def load_handler_bundle(bundle_name: str | None) -> None:
    """
    Import the handler modules in the specified Dag bundle.

    This function does nothing if *bundle_name* is falsy (no handler bundle
    configured) or already loaded. Resolution goes through ``DagBundlesManager``
    so the bundle inherits download/refresh/versioning like any other Dag
    bundle. Imports are idempotent and at most once per-process, same as normal
    Python imports.
    """
    if not bundle_name or bundle_name in _loaded_bundles:
        return

    # Make this lazy so the outer task_handlers package (which re-exports
    # load_handler_bundle) imports cleanly without pulling in Core.
    from airflow.dag_processing.bundles.manager import DagBundlesManager  # noqa: SDK002

    bundle = DagBundlesManager().get_bundle(bundle_name)
    bundle.initialize()
    _import_python_files(pathlib.Path(bundle.path))
    _loaded_bundles.add(bundle_name)
    log.debug("Loaded task-handler bundle", bundle=bundle_name, path=str(bundle.path))


def _import_python_files(root: pathlib.Path) -> None:
    """Import every .py under *root* so module-level decorators run."""
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        if (module_name := get_unique_dag_module_name(str(path))) in sys.modules:
            continue
        if (spec := importlib.util.spec_from_file_location(module_name, path)) is None or spec.loader is None:
            continue
        spec.loader.exec_module(module := importlib.util.module_from_spec(spec))
        sys.modules[module_name] = module
