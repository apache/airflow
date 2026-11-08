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
"""Guard against response fields that pydantic validates lazily.

Pydantic validates an ``Iterable[...]`` field lazily, while FastAPI serializes the
response on the event loop: each item's attributes are then resolved there, on the sync
engine, so a lazy relationship or a ``selectin`` post-load becomes a query that blocks
the whole API server once the connection pool is busy. ``list[...]`` validates on
construction, inside the route, and renders to the same ``type: array`` in the OpenAPI
schema, so the two annotations read as interchangeable while only one of them is safe to
hand a query.

Every pydantic model under ``airflow.api_fastapi`` is checked, not only the collection
responses: the hazard is the annotation, not the name of the class holding it. A model
defined inside a function is out of reach, and so is a response model that lives outside
the package, such as one belonging to a provider's auth manager app.

Some of the annotations are deliberate: #57878 converted a batch of fields to spare their
routes a ``list()`` or ``.all()`` that SQLA2's typing had made them add. Whether that
costs anything is decided at the route, not here, so the fields that already carry the
annotation are listed in ``LAZY_FIELDS_WITHOUT_SYNC_QUERY`` and stay as they are. The
annotation still permits the hazard in all of them, which no test can see from the
datamodel.
"""

from __future__ import annotations

import functools
import importlib
import pkgutil
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from pathlib import Path
from types import ModuleType
from typing import Annotated, Any, get_args, get_origin

import pydantic
import pytest
from pydantic import BaseModel, Field
from typing_extensions import TypeAliasType

import airflow.api_fastapi

from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH

# Iterable and Generator are validated lazily. The other three raise
# PydanticSchemaGenerationError at class definition unless the model sets
# ``arbitrary_types_allowed``, and then Iterator is passed through unvalidated and
# consumed during serialization, the same hazard; the async two cannot validate a sync
# iterable at all. All five are listed to keep the rule one thing to remember.
LAZY_ORIGINS = frozenset({Iterable, Iterator, Generator, AsyncIterable, AsyncIterator})

PACKAGE_PREFIX = f"{airflow.api_fastapi.__name__}."

# None of these defines a model. Importing ``main`` builds a whole app and sets
# ``_AIRFLOW_PROCESS_CONTEXT=server`` for the rest of the process, which would change what
# every later test in the same session sees; the gunicorn modules import the optional
# ``[gunicorn]`` extra at module level.
SKIPPED_MODULES = frozenset(
    {
        f"{PACKAGE_PREFIX}main",
        f"{PACKAGE_PREFIX}gunicorn_app",
        f"{PACKAGE_PREFIX}gunicorn_config",
    }
)


_CORE_DATAMODELS = "airflow-core/src/airflow/api_fastapi/core_api/datamodels"
_EXECUTION_DATAMODELS = "airflow-core/src/airflow/api_fastapi/execution_api/datamodels"

# Lazy annotations that nothing unresolved reaches the event loop through. Materializing
# the container is not enough to earn a place here: pydantic still validates each item
# during serialization, so an ORM row whose attributes the response reads would be
# lazy-loaded there anyway. What makes these safe is that every route hands the field
# items that are already validated responses, plain values, or ORM rows with everything
# the response reads already loaded. A route that passes an unconsumed
# ``session.scalars(...)`` also needs a statement with no post-load: SQLAlchemy runs
# ``selectin``, ``subquery`` and ``immediate`` loads on the first iteration, which here is
# serialization, whether or not the response reads them. The grouping below says which
# reason applies to each entry.
#
# This set is closed. A new field goes in as list[...], not in here.
LAZY_FIELDS_WITHOUT_SYNC_QUERY = frozenset(
    {
        # The route builds the response objects itself, so validation has nothing to
        # resolve. ``task_coordinate_responses`` and ``serialize_asset_events`` both
        # return a list of responses, and prefetch what building them needs.
        f"{_CORE_DATAMODELS}/assets.py::AssetEventCollectionResponse.asset_events",
        f"{_CORE_DATAMODELS}/dag_bundles.py::DagBundleCollectionResponse.dag_bundles",
        f"{_CORE_DATAMODELS}/dag_bundles.py::DagBundleFileCollectionResponse.dag_bundle_files",
        f"{_CORE_DATAMODELS}/event_logs.py::EventLogCollectionResponse.event_logs",
        f"{_CORE_DATAMODELS}/hitl.py::HITLDetailCollection.hitl_details",
        f"{_CORE_DATAMODELS}/task_instances.py::ClearTaskInstanceCollectionResponse.task_instances",
        f"{_CORE_DATAMODELS}/task_instances.py::TaskInstanceCollectionResponse.task_instances",
        f"{_CORE_DATAMODELS}/xcom.py::XComCollectionResponse.xcom_entries",
        # ORM rows, but the response reads only their own columns. ``DeadlineAlertResponse``
        # reaches into ``reference`` with an AliasPath, which is a JSON column, not a
        # relationship.
        f"{_CORE_DATAMODELS}/assets.py::AssetAliasCollectionResponse.asset_aliases",
        f"{_CORE_DATAMODELS}/connections.py::ConnectionCollectionResponse.connections",
        f"{_CORE_DATAMODELS}/import_error.py::ImportErrorCollectionResponse.import_errors",
        f"{_CORE_DATAMODELS}/ui/deadline.py::DeadlineAlertCollectionResponse.deadline_alerts",
        f"{_CORE_DATAMODELS}/variables.py::VariableCollectionResponse.variables",
        # ORM rows, and every relationship the response reads is loaded inside the route.
        # ``Backfill.dag_model``, ``DagVersion.dag_model``, ``DagWarning.dag_model`` and
        # ``Job.dag_model`` are read through an AliasPath and joinedloaded; so is
        # ``DagVersion.bundle``, which ``bundle_url`` reads. The joinedload on
        # ``DagWarning.dag_model`` also replaces its mapper-level ``lazy="selectin"``.
        # ``Job.job_teams`` is ``lazy="selectin"`` and feeds ``team_names``; its post-load
        # runs in the route because that route materializes the result with ``.all()``.
        f"{_CORE_DATAMODELS}/backfills.py::BackfillCollectionResponse.backfills",
        f"{_CORE_DATAMODELS}/dag_versions.py::DAGVersionCollectionResponse.dag_versions",
        f"{_CORE_DATAMODELS}/dag_warning.py::DAGWarningCollectionResponse.dag_warnings",
        f"{_CORE_DATAMODELS}/job.py::JobCollectionResponse.jobs",
        # A JSON column's value, already a list by the time the response is built.
        f"{_EXECUTION_DATAMODELS}/hitl.py::HITLDetailResponse.chosen_options",
        # A generator, but over a result its route already awaited on the async engine.
        f"{_EXECUTION_DATAMODELS}/taskinstance.py::TaskBreadcrumbsResponse.breadcrumbs",
    }
)


def _import_api_fastapi_modules() -> list[ModuleType]:
    modules = [airflow.api_fastapi]
    for info in pkgutil.walk_packages(airflow.api_fastapi.__path__, PACKAGE_PREFIX):
        if info.name in SKIPPED_MODULES:
            continue
        modules.append(importlib.import_module(info.name))
    return modules


def _find_nested_models(
    model: type[pydantic.BaseModel], source_file: str
) -> Iterable[tuple[str, type[pydantic.BaseModel]]]:
    """Models declared inside *model*'s own body, which a module's namespace does not expose."""
    for name, obj in vars(model).items():
        if (
            isinstance(obj, type)
            and issubclass(obj, pydantic.BaseModel)
            and obj.__qualname__ == f"{model.__qualname__}.{name}"
        ):
            yield f"{source_file}::{obj.__qualname__}", obj
            yield from _find_nested_models(obj, source_file)


@functools.cache
def _find_api_fastapi_models() -> tuple[tuple[str, type[pydantic.BaseModel]], ...]:
    """Every pydantic model defined under ``airflow.api_fastapi``, as ``<file>::<Qualified.name>``.

    Reading each module's namespace rather than ``pydantic.BaseModel.__subclasses__()``
    keeps the result independent of what else the test session has imported: a model
    built by ``create_model`` elsewhere -- ``make_partial_model``, or a model defined in
    another test module -- is attributed to the module that *called* it, so it would show
    up as a subclass and make these tests order-dependent.

    ``__module__`` and ``__qualname__`` have to agree with where the name was found, so a
    re-export or an alias is counted once, under its defining module. The key names the
    file rather than the module, and the qualified name disambiguates the class names that
    the core and execution APIs share.
    """
    models = []
    for module in _import_api_fastapi_modules():
        if not (module_file := getattr(module, "__file__", None)):
            continue
        source_file = Path(module_file).resolve().relative_to(AIRFLOW_ROOT_PATH.resolve()).as_posix()
        for name, obj in vars(module).items():
            if (
                isinstance(obj, type)
                and issubclass(obj, pydantic.BaseModel)
                and obj.__module__ == module.__name__
                and obj.__qualname__ == name
            ):
                models.append((f"{source_file}::{name}", obj))
                models.extend(_find_nested_models(obj, source_file))
    return tuple(models)


def _is_lazily_validated(annotation: Any) -> bool:
    """Whether *annotation* is, or wraps, an iterable pydantic validates lazily."""
    # A bare ``Iterable`` is the ABC itself, so it has no origin of its own.
    if any(annotation is lazy for lazy in LAZY_ORIGINS):
        return True
    if get_origin(annotation) in LAZY_ORIGINS:
        return True
    # A TypeAliasType keeps its target in ``__value__`` and reports no origin and no args.
    if (aliased := getattr(annotation, "__value__", None)) is not None:
        return _is_lazily_validated(aliased)
    # Recursing through the arguments makes ``Iterable[X] | None`` and
    # ``Annotated[Iterable[X], Field(...)]`` match as well.
    return any(_is_lazily_validated(arg) for arg in get_args(annotation))


def _rebuild_if_incomplete(model: type[pydantic.BaseModel]) -> None:
    """Resolve the annotations of a model that referenced a name defined after it.

    Such a model stays incomplete until something rebuilds it -- FastAPI does, when it
    builds the app -- and until then its fields hold the unparsed ``ForwardRef``, which no
    amount of ``get_origin`` will see through.
    """
    if not model.__pydantic_complete__:
        model.model_rebuild(raise_errors=False)


def _find_lazy_fields(models: Sequence[tuple[str, type[pydantic.BaseModel]]]) -> frozenset[str]:
    """``<model key>.<field>`` of every field in *models* that pydantic validates lazily."""
    found = set()
    for qualified_name, model in models:
        _rebuild_if_incomplete(model)
        for field_name, field in model.model_fields.items():
            if _is_lazily_validated(field.annotation):
                found.add(f"{qualified_name}.{field_name}")
    return frozenset(found)


@functools.cache
def _find_api_fastapi_lazy_fields() -> frozenset[str]:
    return _find_lazy_fields(_find_api_fastapi_models())


class _ModelWithForwardReference(BaseModel):
    """Its annotation names a model defined after it, so it is incomplete until rebuilt."""

    items: Iterable[_DefinedAfterwards]


class _DefinedAfterwards(BaseModel):
    value: str


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        pytest.param(Iterable[str], True, id="iterable"),
        pytest.param(Iterable, True, id="bare-iterable"),
        pytest.param(Generator[str, None, None], True, id="generator"),
        pytest.param(Generator, True, id="bare-generator"),
        pytest.param(Iterator[str], True, id="iterator"),
        pytest.param(AsyncIterable[str], True, id="async-iterable"),
        pytest.param(AsyncIterator[str], True, id="async-iterator"),
        pytest.param(Iterable[str] | None, True, id="optional"),
        pytest.param(Annotated[Iterable[str], Field(description="x")], True, id="annotated"),
        pytest.param(Annotated[Iterable[str], Field(description="x")] | None, True, id="annotated-optional"),
        pytest.param(dict[str, Iterable[str]], True, id="nested-in-dict"),
        pytest.param(TypeAliasType("LazyAlias", Iterable[str]), True, id="type-alias"),
        pytest.param(str, False, id="scalar"),
        pytest.param(None, False, id="none"),
        pytest.param(list[str], False, id="list"),
        pytest.param(list[str] | None, False, id="optional-list"),
        pytest.param(tuple[str, ...], False, id="tuple"),
        pytest.param(set[str], False, id="set"),
        pytest.param(dict[str, str], False, id="dict"),
        pytest.param(Mapping[str, str], False, id="mapping"),
        pytest.param(Sequence[str], False, id="sequence"),
        pytest.param(Annotated[list[str], Field(description="x")], False, id="annotated-list"),
        pytest.param(TypeAliasType("EagerAlias", list[str]), False, id="type-alias-list"),
    ],
)
def test_is_lazily_validated(annotation, expected):
    assert _is_lazily_validated(annotation) is expected


def test_find_lazy_fields_sees_through_a_forward_reference():
    # The premise: nothing has rebuilt the model yet, so its annotation is still unparsed.
    assert not _ModelWithForwardReference.__pydantic_complete__
    assert _find_lazy_fields([("forward_reference", _ModelWithForwardReference)]) == {
        "forward_reference.items"
    }


def test_find_lazy_fields_reaches_a_nested_model():
    class Outer(BaseModel):
        class Inner(BaseModel):
            items: Iterable[str]

    nested = list(_find_nested_models(Outer, "some/datamodels/thing.py"))
    assert _find_lazy_fields(nested) == {f"some/datamodels/thing.py::{Outer.Inner.__qualname__}.items"}


def test_all_models_resolve_their_annotations():
    """Every model must be built, or a lazy field could hide inside a ``ForwardRef``."""
    incomplete = []
    for qualified_name, model in _find_api_fastapi_models():
        _rebuild_if_incomplete(model)
        if not model.__pydantic_complete__:
            incomplete.append(qualified_name)
    assert not incomplete, (
        "Model(s) whose annotations cannot be resolved: "
        + ", ".join(sorted(incomplete))
        + ".\nTheir fields cannot be inspected, so a lazy field there would go unnoticed."
    )


def test_unlisted_response_fields_are_not_lazy():
    models = _find_api_fastapi_models()
    # Keeps this from passing vacuously if the package moves or the filter above narrows.
    assert len(models) > 100, f"only {len(models)} models discovered under {airflow.api_fastapi.__name__}"

    offenders = sorted(_find_api_fastapi_lazy_fields() - LAZY_FIELDS_WITHOUT_SYNC_QUERY)
    if offenders:
        pytest.fail(
            "These response fields are validated lazily, so the items are resolved while FastAPI "
            "serializes the response on the event loop and any query they trigger there blocks the "
            "whole API server. Declare them as list[...]; the OpenAPI schema is unchanged. "
            "LAZY_FIELDS_WITHOUT_SYNC_QUERY is not the place for a new one: it records the fields "
            "that predate this test, and a route that resolves its items today is one refactor "
            "away from not doing so:\n" + "\n".join(offenders),
            pytrace=False,
        )


def test_exempt_fields_are_still_lazy():
    """A field that is no longer lazy has to leave the exempt set, or it silently rots there."""
    stale = sorted(LAZY_FIELDS_WITHOUT_SYNC_QUERY - _find_api_fastapi_lazy_fields())
    assert not stale, (
        "No longer declared lazily, so the entries are dead: "
        + ", ".join(stale)
        + ".\nDelete them from LAZY_FIELDS_WITHOUT_SYNC_QUERY."
    )
