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
Cadwyn versioning and in-process migration for the supervisor schemas.

Two distinct Cadwyn ``VersionBundle`` instances coexist in the codebase:

* :data:`.versions.bundle` (this package) — versions the wire shapes the
  Task SDK supervisor exchanges with a lang-SDK runtime subprocess
  launched by a coordinator (Java, Go, Rust, ...). The bodies it
  references live in their semantic homes
  (``airflow.sdk.execution_time.comms`` for task execution,
  ``airflow.dag_processing.processor`` for Dag parsing); this package
  only owns the versioning machinery, not the model definitions.
* :data:`airflow.api_fastapi.execution_api.versions.bundle` — versions
  the HTTP contract between Task SDK clients and the API server.
  Unaffected by this package.

:func:`registered_models_by_name` resolves a wire-shape ``type``
discriminator to the head Pydantic class. It is computed dynamically
from the four discriminated unions ``ToTask``, ``ToSupervisor``
(task-execution channel) and ``ToManager``, ``ToDagProcessor``
(dag-processing channel) so the registry is always in sync with the
actual unions ``CommsDecoder`` decodes against -- no hand-maintained
list to drift. Triggerer unions are intentionally excluded (the
Triggerer channel is not handled by lang-SDK coordinators today).
"""

from __future__ import annotations

import functools
from types import UnionType
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin

from pydantic import BaseModel

from airflow.sdk.execution_time.schema.migrator import (
    SchemaVersionMigrator,
    get_schema_version_migrator,
)

if TYPE_CHECKING:
    from collections.abc import Iterable


def _iter_model_types(t: object) -> Iterable[type[BaseModel]]:
    """Generate BaseModel subclass included in type."""
    origin = get_origin(t)
    if origin is Annotated:
        yield from _iter_model_types(get_args(t)[0])
    elif origin is UnionType:
        for member in get_args(t):
            yield from _iter_model_types(member)
    elif isinstance(t, type) and issubclass(t, BaseModel):
        yield t


@functools.cache
def registered_models_by_name() -> dict[str, type[BaseModel]]:
    """
    Map every supervisor schema body's class name to the head Pydantic class.

    Single source of truth for the registry. Built once by walking the
    four discriminated unions the supervisor decodes against; cached
    per-process because the registry only changes when a union member
    is added in ``comms.py`` or ``processor.py`` (which needs a
    restart anyway). :func:`resolve_body_class` looks up the wire-shape
    ``type`` discriminator against it.

    Imports are deferred so this package stays cheap to import for
    callers that only need the bundle or migrator (e.g. the migrator
    singleton factory); pulling in ``processor`` eagerly would drag the
    whole DAG-processor import graph into every consumer.

    Raises ``RuntimeError`` if two distinct classes register under the
    same ``__name__`` -- the wire discriminator must round-trip to a
    single head class, so a name clash is a programmer error that must
    surface immediately rather than silently picking a winner.
    """
    from airflow.dag_processing.processor import ToDagProcessor, ToManager
    from airflow.sdk.execution_time.comms import ToSupervisor, ToTask

    by_name: dict[str, type[BaseModel]] = {}
    for source in (ToTask, ToSupervisor, ToManager, ToDagProcessor):
        for model in _iter_model_types(source):
            existing = by_name.get(model.__name__)
            if existing is None:
                by_name[model.__name__] = model
            elif existing is not model:
                raise RuntimeError(
                    f"Duplicate supervisor schema body name {model.__name__!r}: "
                    f"both {existing!r} and {model!r} register the same wire type"
                )
    return by_name


def resolve_body_class(body: Any) -> type[BaseModel] | None:
    """Resolve a wire-body dict's ``type`` discriminator to its head Pydantic class."""
    if not isinstance(body, dict):
        return None
    name = body.get("type")
    if not isinstance(name, str):
        return None
    return registered_models_by_name().get(name)


def __getattr__(name: str) -> Any:
    # Re-export ``bundle`` lazily so importing this package does not import ``cadwyn`` (-> FastAPI/
    # Starlette) until something actually accesses ``schema.bundle``. The Task SDK supervisor imports
    # this package on every worker, but only the foreign-language-SDK migration path touches the
    # bundle, so a pure-Python worker never pays the cadwyn import.
    if name == "bundle":
        from airflow.sdk.execution_time.schema.versions import get_bundle

        return get_bundle()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "SchemaVersionMigrator",
    "bundle",
    "get_schema_version_migrator",
    "registered_models_by_name",
    "resolve_body_class",
]
