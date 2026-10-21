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

"""Python implementations of YAML ``run:`` tasks."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import pathlib
import sys
import threading
from collections.abc import Callable
from types import ModuleType
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class TaskHandlerRegistry:
    """Process-local registry of handlers, keyed by Dag and task ID."""

    def __init__(self) -> None:
        self._handlers: dict[tuple[str, str], Callable[..., Any]] = {}
        self._handler_modules: dict[tuple[str, str], str] = {}
        self._loaded_bundles: dict[str, tuple[pathlib.Path, tuple[tuple[pathlib.Path, int, int], ...]]] = {}
        self._lock = threading.RLock()

    def register(self, dag_id: str, task_id: str, handler: Callable[..., Any]) -> None:
        key = (dag_id, task_id)
        module_name = handler.__module__
        with self._lock:
            if key in self._handlers and self._handler_modules[key] != module_name:
                raise ValueError(f"A task handler is already registered for Dag {dag_id!r}, task {task_id!r}")
            self._handlers[key] = handler
            self._handler_modules[key] = module_name

    def get(self, dag_id: str, task_id: str) -> Callable[..., Any] | None:
        with self._lock:
            return self._handlers.get((dag_id, task_id))

    def describe(self, dag_id: str, task_id: str) -> inspect.Signature | None:
        handler = self.get(dag_id, task_id)
        return inspect.signature(handler) if handler is not None else None

    def clear(self) -> None:
        """Clear registered handlers. Intended for isolated parser and test lifecycles."""
        with self._lock:
            self._handlers.clear()
            self._handler_modules.clear()
            self._loaded_bundles.clear()

    def load_from_path(self, root: str | pathlib.Path, *, bundle_name: str | None = None) -> None:
        """Import Python modules under *root* so their decorated handlers register."""
        root_path = pathlib.Path(root).resolve()
        if not root_path.is_dir():
            raise FileNotFoundError(f"Task handler bundle path {root_path} is not a directory")
        root_text = str(root_path)
        if root_text not in sys.path:
            sys.path.insert(0, root_text)
        identity = bundle_name or root_text
        package_name = f"_airflow_task_handlers_{hashlib.sha256(identity.encode()).hexdigest()[:16]}"
        sources = []
        for source in sorted(root_path.rglob("*.py")):
            relative = source.relative_to(root_path)
            if any(part.startswith(".") or part == "__pycache__" for part in relative.parts):
                continue
            if source.name == "__init__.py" or not all(
                part.isidentifier() for part in relative.with_suffix("").parts
            ):
                continue
            stat = source.stat()
            sources.append((source, stat.st_mtime_ns, stat.st_size))
        fingerprint = tuple(sources)

        with self._lock:
            if self._loaded_bundles.get(package_name) == (root_path, fingerprint):
                return
            for key, module_name in tuple(self._handler_modules.items()):
                if module_name.startswith(f"{package_name}."):
                    self._handlers.pop(key, None)
                    self._handler_modules.pop(key, None)
            for module_name in tuple(sys.modules):
                if module_name == package_name or module_name.startswith(f"{package_name}."):
                    sys.modules.pop(module_name)
            package = ModuleType(package_name)
            package.__path__ = [root_text]
            package.__package__ = package_name
            sys.modules[package_name] = package
            for source, _, _ in sources:
                module_parts = (package_name, *source.relative_to(root_path).with_suffix("").parts)
                importlib.import_module(".".join(module_parts))
            self._loaded_bundles[package_name] = (root_path, fingerprint)


task_handler_registry = TaskHandlerRegistry()


def task_handler(*, dag_id: str, task_id: str) -> Callable[[F], F]:
    """
    Register a Python function as the body of a YAML Dag's ``run:`` task.

    The decorator leaves the function callable so it can also be tested directly.
    """
    if not dag_id or not task_id:
        raise ValueError("dag_id and task_id must be non-empty strings")

    def register(handler: F) -> F:
        if not callable(handler):
            raise TypeError("@task_handler can only decorate a callable")
        task_handler_registry.register(dag_id, task_id, handler)
        return handler

    return register


__all__ = ["TaskHandlerRegistry", "task_handler", "task_handler_registry"]
