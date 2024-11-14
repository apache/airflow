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
    SupportsIndex,
    ValuesView,
)

import attrs
import lazy_object_proxy
from sqlalchemy import select

from airflow.datasets import (
    Dataset,
    DatasetAlias,
    DatasetAliasEvent,
    extract_event_key,
)
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.models.dataset import DatasetAliasModel, DatasetEvent, DatasetModel
from airflow.utils.db import LazySelectSequence
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from sqlalchemy.engine import Row
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.expression import Select, TextClause

    from airflow.models.baseoperator import BaseOperator

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
    "outlet_events",
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
class OutletEventAccessor:
    """
    Wrapper to access an outlet dataset event in template.

    :meta private:
    """

    raw_key: str | Dataset | DatasetAlias
    extra: dict[str, Any] = attrs.Factory(dict)
    dataset_alias_events: list[DatasetAliasEvent] = attrs.field(factory=list)

    def add(self, dataset: Dataset | str, extra: dict[str, Any] | None = None) -> None:
        """Add a DatasetEvent to an existing Dataset."""
        if isinstance(dataset, str):
            warnings.warn(
                (
                    "Emitting dataset events using string is deprecated and will be removed in Airflow 3. "
                    "Please use the Dataset object (renamed as Asset in Airflow 3) directly"
                ),
                DeprecationWarning,
                stacklevel=2,
            )
            dataset_uri = dataset
        elif isinstance(dataset, Dataset):
            dataset_uri = dataset.uri
        else:
            return

        if isinstance(self.raw_key, str):
            dataset_alias_name = self.raw_key
        elif isinstance(self.raw_key, DatasetAlias):
            dataset_alias_name = self.raw_key.name
        else:
            return

        event = DatasetAliasEvent(
            source_alias_name=dataset_alias_name, dest_dataset_uri=dataset_uri, extra=extra or {}
        )
        self.dataset_alias_events.append(event)


class OutletEventAccessors(Mapping[str, OutletEventAccessor]):
    """
    Lazy mapping of outlet dataset event accessors.

    :meta private:
    """

    def __init__(self) -> None:
        self._dict: dict[str, OutletEventAccessor] = {}

    def __str__(self) -> str:
        return f"OutletEventAccessors(_dict={self._dict})"

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __getitem__(self, key: str | Dataset | DatasetAlias) -> OutletEventAccessor:
        if isinstance(key, str):
            warnings.warn(
                (
                    "Accessing outlet_events using string is deprecated and will be removed in Airflow 3. "
                    "Please use the Dataset or DatasetAlias object (renamed as Asset and AssetAlias in Airflow 3) directly"
                ),
                DeprecationWarning,
                stacklevel=2,
            )

        event_key = extract_event_key(key)
        if event_key not in self._dict:
            self._dict[event_key] = OutletEventAccessor(extra={}, raw_key=key)
        return self._dict[event_key]


class LazyDatasetEventSelectSequence(LazySelectSequence[DatasetEvent]):
    """
    List-like interface to lazily access DatasetEvent rows.

    :meta private:
    """

    @staticmethod
    def _rebuild_select(stmt: TextClause) -> Select:
        return select(DatasetEvent).from_statement(stmt)

    @staticmethod
    def _process_row(row: Row) -> DatasetEvent:
        return row[0]


@attrs.define(init=False)
class InletEventsAccessors(Mapping[str, LazyDatasetEventSelectSequence]):
    """
    Lazy mapping for inlet dataset events accessors.

    :meta private:
    """

    _inlets: list[Any]
    _datasets: dict[str, Dataset]
    _dataset_aliases: dict[str, DatasetAlias]
    _session: Session

    def __init__(self, inlets: list, *, session: Session) -> None:
        self._inlets = inlets
        self._session = session
        self._datasets = {}
        self._dataset_aliases = {}

        for inlet in inlets:
            if isinstance(inlet, Dataset):
                self._datasets[inlet.uri] = inlet
            elif isinstance(inlet, DatasetAlias):
                self._dataset_aliases[inlet.name] = inlet

    def __iter__(self) -> Iterator[str]:
        return iter(self._inlets)

    def __len__(self) -> int:
        return len(self._inlets)

    def __getitem__(self, key: int | str | Dataset | DatasetAlias) -> LazyDatasetEventSelectSequence:
        if isinstance(key, int):  # Support index access; it's easier for trivial cases.
            obj = self._inlets[key]
            if not isinstance(obj, (Dataset, DatasetAlias)):
                raise IndexError(key)
        else:
            obj = key

        if isinstance(obj, DatasetAlias):
            dataset_alias = self._dataset_aliases[obj.name]
            join_clause = DatasetEvent.source_aliases
            where_clause = DatasetAliasModel.name == dataset_alias.name
        elif isinstance(obj, (Dataset, str)):
            if isinstance(obj, str):
                warnings.warn(
                    (
                        "Accessing inlet_events using string is deprecated and will be removed in Airflow 3. "
                        "Please use the Dataset object (renamed as Asset in Airflow 3) directly"
                    ),
                    DeprecationWarning,
                    stacklevel=2,
                )
            dataset = self._datasets[extract_event_key(obj)]
            join_clause = DatasetEvent.dataset
            where_clause = DatasetModel.uri == dataset.uri
        else:
            raise ValueError(key)

        return LazyDatasetEventSelectSequence.from_select(
            select(DatasetEvent).join(join_clause).where(where_clause),
            order_by=[DatasetEvent.timestamp],
            session=self._session,
        )


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
    """
    Jinja2 template context for task rendering.

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
        """
        Pickle the context as a dict.

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
    """
    Merge parameters into an existing context.

    Like ``dict.update()`` , this take the same parameters, and updates
    ``context`` in-place.

    This is implemented as a free function because the ``Context`` type is
    "faked" as a ``TypedDict`` in ``context.pyi``, which cannot have custom
    functions.

    :meta private:
    """
    context.update(*args, **kwargs)


def context_update_for_unmapped(context: Context, task: BaseOperator) -> None:
    """
    Update context after task unmapping.

    Since ``get_template_context()`` is called before unmapping, the context
    contains information about the mapped task. We need to do some in-place
    updates to ensure the template context reflects the unmapped task instead.

    :meta private:
    """
    from airflow.models.param import process_params

    context["task"] = context["ti"].task = task
    context["params"] = process_params(context["dag"], task, context["dag_run"], suppress_exception=False)


def context_copy_partial(source: Context, keys: Container[str]) -> Context:
    """
    Create a context by copying items under selected keys in ``source``.

    This is implemented as a free function because the ``Context`` type is
    "faked" as a ``TypedDict`` in ``context.pyi``, which cannot have custom
    functions.

    :meta private:
    """
    new = Context({k: v for k, v in source._context.items() if k in keys})
    new._deprecation_replacements = source._deprecation_replacements.copy()
    return new


def lazy_mapping_from_context(source: Context) -> Mapping[str, Any]:
    """
    Create a mapping that wraps deprecated entries in a lazy object proxy.

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


def context_get_outlet_events(context: Context) -> OutletEventAccessors:
    try:
        return context["outlet_events"]
    except KeyError:
        return OutletEventAccessors()
