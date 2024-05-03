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
"""Jinja2 template rendering context helper."""

from __future__ import annotations

import contextlib
import copy
import functools
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Container,
    ItemsView,
    Iterator,
    KeysView,
    Mapping,
    MutableMapping,
    Sequence,
    SupportsIndex,
    ValuesView,
    overload,
)

import attrs
import lazy_object_proxy

from airflow.datasets import Dataset, coerce_to_uri
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.expression import Select

    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dataset import DatasetEvent

# NOTE: Please keep this in sync with the following:
# * Context in airflow/utils/context.pyi.
# * Table in docs/apache-airflow/templates-ref.rst
KNOWN_CONTEXT_KEYS: set[str] = {
    "conf",
    "conn",
    "dag",
    "dag_run",
    "data_interval_end",
    "data_interval_start",
    "dataset_events",
    "ds",
    "ds_nodash",
    "execution_date",
    "expanded_ti_count",
    "exception",
    "inlets",
    "inlet_events",
    "logical_date",
    "macros",
    "map_index_template",
    "next_ds",
    "next_ds_nodash",
    "next_execution_date",
    "outlets",
    "params",
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
    "prev_ds",
    "prev_ds_nodash",
    "prev_execution_date",
    "prev_execution_date_success",
    "prev_start_date_success",
    "prev_end_date_success",
    "reason",
    "run_id",
    "task",
    "task_instance",
    "task_instance_key_str",
    "test_mode",
    "templates_dict",
    "ti",
    "tomorrow_ds",
    "tomorrow_ds_nodash",
    "triggering_dataset_events",
    "ts",
    "ts_nodash",
    "ts_nodash_with_tz",
    "try_number",
    "var",
    "yesterday_ds",
    "yesterday_ds_nodash",
}


class VariableAccessor:
    """Wrapper to access Variable values in template."""

    def __init__(self, *, deserialize_json: bool) -> None:
        self._deserialize_json = deserialize_json
        self.var: Any = None

    def __getattr__(self, key: str) -> Any:
        from airflow.models.variable import Variable

        self.var = Variable.get(key, deserialize_json=self._deserialize_json)
        return self.var

    def __repr__(self) -> str:
        return str(self.var)

    def get(self, key, default: Any = NOTSET) -> Any:
        from airflow.models.variable import Variable

        if default is NOTSET:
            return Variable.get(key, deserialize_json=self._deserialize_json)
        return Variable.get(key, default, deserialize_json=self._deserialize_json)


class ConnectionAccessor:
    """Wrapper to access Connection entries in template."""

    def __init__(self) -> None:
        self.var: Any = None

    def __getattr__(self, key: str) -> Any:
        from airflow.models.connection import Connection

        self.var = Connection.get_connection_from_secrets(key)
        return self.var

    def __repr__(self) -> str:
        return str(self.var)

    def get(self, key: str, default_conn: Any = None) -> Any:
        from airflow.exceptions import AirflowNotFoundException
        from airflow.models.connection import Connection

        try:
            return Connection.get_connection_from_secrets(key)
        except AirflowNotFoundException:
            return default_conn


@attrs.define()
class DatasetEventAccessor:
    """Wrapper to access a DatasetEvent instance in template."""

    extra: dict[str, Any]


class DatasetEventAccessors(Mapping[str, DatasetEventAccessor]):
    """Lazy mapping of dataset event accessors."""

    def __init__(self) -> None:
        self._dict: dict[str, DatasetEventAccessor] = {}

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __getitem__(self, key: str | Dataset) -> DatasetEventAccessor:
        if (uri := coerce_to_uri(key)) not in self._dict:
            self._dict[uri] = DatasetEventAccessor({})
        return self._dict[uri]


@attrs.define()
class InletEventsAccessor(Sequence["DatasetEvent"]):
    """Lazy sequence to access inlet dataset events.

    :meta private:
    """

    _uri: str
    _session: Session

    def _get_select_stmt(self, *, reverse: bool = False) -> Select:
        from sqlalchemy import select

        from airflow.models.dataset import DatasetEvent, DatasetModel

        stmt = select(DatasetEvent).join(DatasetEvent.dataset).where(DatasetModel.uri == self._uri)
        if reverse:
            return stmt.order_by(DatasetEvent.timestamp.desc())
        return stmt.order_by(DatasetEvent.timestamp.asc())

    def __reversed__(self) -> Iterator[DatasetEvent]:
        return iter(self._session.scalar(self._get_select_stmt(reverse=True)))

    def __iter__(self) -> Iterator[DatasetEvent]:
        return iter(self._session.scalar(self._get_select_stmt()))

    @overload
    def __getitem__(self, key: int) -> DatasetEvent: ...

    @overload
    def __getitem__(self, key: slice) -> Sequence[DatasetEvent]: ...

    def __getitem__(self, key: int | slice) -> DatasetEvent | Sequence[DatasetEvent]:
        if not isinstance(key, int):
            raise ValueError("non-index access is not supported")
        if key >= 0:
            stmt = self._get_select_stmt().offset(key)
        else:
            stmt = self._get_select_stmt(reverse=True).offset(-1 - key)
        if (event := self._session.scalar(stmt.limit(1))) is not None:
            return event
        raise IndexError(key)

    def __len__(self) -> int:
        from sqlalchemy import func, select

        return self._session.scalar(select(func.count()).select_from(self._get_select_stmt()))


@attrs.define(init=False)
class InletEventsAccessors(Mapping[str, InletEventsAccessor]):
    """Lazy mapping for inlet dataset events accessors.

    :meta private:
    """

    _inlets: list[Any]
    _datasets: dict[str, Dataset]
    _session: Session

    def __init__(self, inlets: list, *, session: Session) -> None:
        self._inlets = inlets
        self._datasets = {inlet.uri: inlet for inlet in inlets if isinstance(inlet, Dataset)}
        self._session = session

    def __iter__(self) -> Iterator[str]:
        return iter(self._inlets)

    def __len__(self) -> int:
        return len(self._inlets)

    def __getitem__(self, key: int | str | Dataset) -> InletEventsAccessor:
        if isinstance(key, int):  # Support index access; it's easier for trivial cases.
            dataset = self._inlets[key]
            if not isinstance(dataset, Dataset):
                raise IndexError(key)
        else:
            dataset = self._datasets[coerce_to_uri(key)]
        return InletEventsAccessor(dataset.uri, session=self._session)


class AirflowContextDeprecationWarning(RemovedInAirflow3Warning):
    """Warn for usage of deprecated context variables in a task."""


def _create_deprecation_warning(key: str, replacements: list[str]) -> RemovedInAirflow3Warning:
    message = f"Accessing {key!r} from the template is deprecated and will be removed in a future version."
    if not replacements:
        return AirflowContextDeprecationWarning(message)
    display_except_last = ", ".join(repr(r) for r in replacements[:-1])
    if display_except_last:
        message += f" Please use {display_except_last} or {replacements[-1]!r} instead."
    else:
        message += f" Please use {replacements[-1]!r} instead."
    return AirflowContextDeprecationWarning(message)


class Context(MutableMapping[str, Any]):
    """Jinja2 template context for task rendering.

    This is a mapping (dict-like) class that can lazily emit warnings when
    (and only when) deprecated context keys are accessed.
    """

    _DEPRECATION_REPLACEMENTS: dict[str, list[str]] = {
        "execution_date": ["data_interval_start", "logical_date"],
        "next_ds": ["{{ data_interval_end | ds }}"],
        "next_ds_nodash": ["{{ data_interval_end | ds_nodash }}"],
        "next_execution_date": ["data_interval_end"],
        "prev_ds": [],
        "prev_ds_nodash": [],
        "prev_execution_date": [],
        "prev_execution_date_success": ["prev_data_interval_start_success"],
        "tomorrow_ds": [],
        "tomorrow_ds_nodash": [],
        "yesterday_ds": [],
        "yesterday_ds_nodash": [],
    }

    def __init__(self, context: MutableMapping[str, Any] | None = None, **kwargs: Any) -> None:
        self._context: MutableMapping[str, Any] = context or {}
        if kwargs:
            self._context.update(kwargs)
        self._deprecation_replacements = self._DEPRECATION_REPLACEMENTS.copy()

    def __repr__(self) -> str:
        return repr(self._context)

    def __reduce_ex__(self, protocol: SupportsIndex) -> tuple[Any, ...]:
        """Pickle the context as a dict.

        We are intentionally going through ``__getitem__`` in this function,
        instead of using ``items()``, to trigger deprecation warnings.
        """
        items = [(key, self[key]) for key in self._context]
        return dict, (items,)

    def __copy__(self) -> Context:
        new = type(self)(copy.copy(self._context))
        new._deprecation_replacements = self._deprecation_replacements.copy()
        return new

    def __getitem__(self, key: str) -> Any:
        with contextlib.suppress(KeyError):
            warnings.warn(
                _create_deprecation_warning(key, self._deprecation_replacements[key]),
                stacklevel=2,
            )
        with contextlib.suppress(KeyError):
            return self._context[key]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._deprecation_replacements.pop(key, None)
        self._context[key] = value

    def __delitem__(self, key: str) -> None:
        self._deprecation_replacements.pop(key, None)
        del self._context[key]

    def __contains__(self, key: object) -> bool:
        return key in self._context

    def __iter__(self) -> Iterator[str]:
        return iter(self._context)

    def __len__(self) -> int:
        return len(self._context)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Context):
            return NotImplemented
        return self._context == other._context

    def __ne__(self, other: Any) -> bool:
        if not isinstance(other, Context):
            return NotImplemented
        return self._context != other._context

    def keys(self) -> KeysView[str]:
        return self._context.keys()

    def items(self):
        return ItemsView(self._context)

    def values(self):
        return ValuesView(self._context)


def context_merge(context: Context, *args: Any, **kwargs: Any) -> None:
    """Merge parameters into an existing context.

    Like ``dict.update()`` , this take the same parameters, and updates
    ``context`` in-place.

    This is implemented as a free function because the ``Context`` type is
    "faked" as a ``TypedDict`` in ``context.pyi``, which cannot have custom
    functions.

    :meta private:
    """
    context.update(*args, **kwargs)


def context_update_for_unmapped(context: Context, task: BaseOperator) -> None:
    """Update context after task unmapping.

    Since ``get_template_context()`` is called before unmapping, the context
    contains information about the mapped task. We need to do some in-place
    updates to ensure the template context reflects the unmapped task instead.

    :meta private:
    """
    from airflow.models.param import process_params

    context["task"] = context["ti"].task = task
    context["params"] = process_params(context["dag"], task, context["dag_run"], suppress_exception=False)


def context_copy_partial(source: Context, keys: Container[str]) -> Context:
    """Create a context by copying items under selected keys in ``source``.

    This is implemented as a free function because the ``Context`` type is
    "faked" as a ``TypedDict`` in ``context.pyi``, which cannot have custom
    functions.

    :meta private:
    """
    new = Context({k: v for k, v in source._context.items() if k in keys})
    new._deprecation_replacements = source._deprecation_replacements.copy()
    return new


def lazy_mapping_from_context(source: Context) -> Mapping[str, Any]:
    """Create a mapping that wraps deprecated entries in a lazy object proxy.

    This further delays deprecation warning to until when the entry is actually
    used, instead of when it's accessed in the context. The result is useful for
    passing into a callable with ``**kwargs``, which would unpack the mapping
    too eagerly otherwise.

    This is implemented as a free function because the ``Context`` type is
    "faked" as a ``TypedDict`` in ``context.pyi``, which cannot have custom
    functions.

    :meta private:
    """
    if not isinstance(source, Context):
        # Sometimes we are passed a plain dict (usually in tests, or in User's
        # custom operators) -- be lienent about what we accept so we don't
        # break anything for users.
        return source

    def _deprecated_proxy_factory(k: str, v: Any) -> Any:
        replacements = source._deprecation_replacements[k]
        warnings.warn(_create_deprecation_warning(k, replacements), stacklevel=2)
        return v

    def _create_value(k: str, v: Any) -> Any:
        if k not in source._deprecation_replacements:
            return v
        factory = functools.partial(_deprecated_proxy_factory, k, v)
        return lazy_object_proxy.Proxy(factory)

    return {k: _create_value(k, v) for k, v in source._context.items()}


def context_get_dataset_events(context: Context) -> DatasetEventAccessors:
    try:
        return context["dataset_events"]
    except KeyError:
        return DatasetEventAccessors()
