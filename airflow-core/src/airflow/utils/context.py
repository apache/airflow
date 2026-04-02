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

import warnings
from typing import Any

from sqlalchemy import select

from airflow.models.asset import AssetModel
from airflow.sdk import Asset
from airflow.sdk.execution_time.context import (
    ConnectionAccessor as ConnectionAccessorSDK,
    OutletEventAccessors as OutletEventAccessorsSDK,
    VariableAccessor as VariableAccessorSDK,
)
from airflow.serialization.definitions.notset import NOTSET, is_arg_set
from airflow.utils.deprecation_tools import DeprecatedImportWarning, add_deprecated_classes
from airflow.utils.session import create_session

warnings.warn(
    "Module airflow.utils.context is deprecated and will be removed in the "
    "future. Use airflow.sdk.execution_time.context if you are using the "
    "classes inside an Airflow task.",
    DeprecatedImportWarning,
    stacklevel=2,
)


class VariableAccessor(VariableAccessorSDK):
    """Wrapper to access Variable values in template."""

    def __getattr__(self, key: str) -> Any:
        from airflow.models.variable import Variable

        return Variable.get(key, deserialize_json=self._deserialize_json)

    def get(self, key, default: Any = NOTSET) -> Any:
        from airflow.models.variable import Variable

        if is_arg_set(default):
            return Variable.get(key, default, deserialize_json=self._deserialize_json)
        return Variable.get(key, deserialize_json=self._deserialize_json)


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

        if asset is None:
            raise ValueError("No active asset found with either name or uri.")
        return Asset(name=asset.name, uri=asset.uri, group=asset.group, extra=asset.extra)


add_deprecated_classes(
    {
        __name__: {
            "KNOWN_CONTEXT_KEYS": "airflow.sdk.definitions.context",
            "Context": "airflow.sdk.definitions.context",
            "context_copy_partial": "airflow.sdk.definitions.context",
            "context_merge": "airflow.sdk.definitions.context",
        },
    },
    package=__name__,
)
