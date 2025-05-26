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

from collections.abc import (
    Container,
)
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from sqlalchemy import select

from airflow.models.asset import (
    AssetModel,
)
from airflow.sdk.definitions.context import Context
from airflow.sdk.execution_time.context import (
    ConnectionAccessor as ConnectionAccessorSDK,
    OutletEventAccessors as OutletEventAccessorsSDK,
    VariableAccessor as VariableAccessorSDK,
)
from airflow.utils.session import create_session
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from airflow.sdk.definitions.asset import Asset

# NOTE: Please keep this in sync with the following:
# * Context in task-sdk/src/airflow/sdk/definitions/context.py
# * Table in docs/apache-airflow/templates-ref.rst
KNOWN_CONTEXT_KEYS: set[str] = {
    "conn",
    "dag",
    "dag_run",
    "data_interval_end",
    "data_interval_start",
    "ds",
    "ds_nodash",
    "expanded_ti_count",
    "exception",
    "inlets",
    "inlet_events",
    "logical_date",
    "macros",
    "map_index_template",
    "outlets",
    "outlet_events",
    "params",
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
    "prev_start_date_success",
    "prev_end_date_success",
    "reason",
    "run_id",
    "start_date",
    "task",
    "task_reschedule_count",
    "task_instance",
    "task_instance_key_str",
    "test_mode",
    "templates_dict",
    "ti",
    "triggering_asset_events",
    "ts",
    "ts_nodash",
    "ts_nodash_with_tz",
    "try_number",
    "var",
}


class VariableAccessor(VariableAccessorSDK):
    """Wrapper to access Variable values in template."""

    def __getattr__(self, key: str) -> Any:
        from airflow.models.variable import Variable

        return Variable.get(key, deserialize_json=self._deserialize_json)

    def get(self, key, default: Any = NOTSET) -> Any:
        from airflow.models.variable import Variable

        if default is NOTSET:
            return Variable.get(key, deserialize_json=self._deserialize_json)
        return Variable.get(key, default, deserialize_json=self._deserialize_json)


class ConnectionAccessor(ConnectionAccessorSDK):
    """Wrapper to access Connection entries in template."""

    def __getattr__(self, conn_id: str) -> Any:
        from airflow.models.connection import Connection

        return Connection.get_connection_from_secrets(conn_id)

    def get(self, conn_id: str, default_conn: Any = None) -> Any:
        from airflow.exceptions import AirflowNotFoundException
        from airflow.models.connection import Connection

        try:
            return Connection.get_connection_from_secrets(conn_id)
        except AirflowNotFoundException:
            return default_conn


class OutletEventAccessors(OutletEventAccessorsSDK):
    """
    Lazy mapping of outlet asset event accessors.

    :meta private:
    """

    @staticmethod
    def _get_asset_from_db(name: str | None = None, uri: str | None = None) -> Asset:
        if name:
            with create_session() as session:
                asset = session.scalar(
                    select(AssetModel).where(AssetModel.name == name, AssetModel.active.has())
                )
        elif uri:
            with create_session() as session:
                asset = session.scalar(
                    select(AssetModel).where(AssetModel.uri == uri, AssetModel.active.has())
                )
        else:
            raise ValueError("Either name or uri must be provided")

        return asset.to_public()


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
    if not context:
        context = Context()

    context.update(*args, **kwargs)


def context_copy_partial(source: Context, keys: Container[str]) -> Context:
    """
    Create a context by copying items under selected keys in ``source``.

    :meta private:
    """
    new = {k: v for k, v in source.items() if k in keys}
    return cast("Context", new)
