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
from __future__ import annotations

import contextlib
import json
import os
from importlib.metadata import version
from typing import TYPE_CHECKING, Any, cast

from packaging.version import Version

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.models import Connection, Operator
from airflow.utils.helpers import prune_dict

try:
    # ImportError has been renamed to ParseImportError in airflow 2.10.0, and since our provider tests should
    # run on all supported versions of Airflow, this compatibility shim falls back to the old ImportError so
    # that tests can import it from here and use it in their code and run against older versions of Airflow
    # This import can be removed (and all tests switched to import ParseImportError directly) as soon as
    # all providers are updated to airflow 2.10+.
    from airflow.models.errors import ParseImportError
except ImportError:
    from airflow.models.errors import ImportError as ParseImportError  # type: ignore[no-redef,attr-defined]

from airflow import __version__ as airflow_version

AIRFLOW_VERSION = Version(airflow_version)
AIRFLOW_V_2_8_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("2.8.0")
AIRFLOW_V_2_9_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("2.9.0")
AIRFLOW_V_2_10_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("2.10.0")
AIRFLOW_V_3_0_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("3.0.0")

try:
    from airflow.models.baseoperatorlink import BaseOperatorLink
except ImportError:
    # Compatibility for Airflow 2.7.*
    from airflow.models.baseoperator import BaseOperatorLink

try:
    from airflow.providers.standard.core.operators.bash import BashOperator
    from airflow.providers.standard.core.sensors.bash import BashSensor
except ImportError:
    # Compatibility for Airflow < 2.10.*
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef,attr-defined]
    from airflow.sensors.bash import BashSensor  # type: ignore[no-redef,attr-defined]


if TYPE_CHECKING:
    from airflow.models.asset import (
        AssetAliasModel,
        AssetDagRunQueue,
        AssetEvent,
        AssetModel,
        DagScheduleAssetReference,
        TaskOutletAssetReference,
    )
else:
    try:
        from airflow.models.asset import (
            AssetAliasModel,
            AssetDagRunQueue,
            AssetEvent,
            AssetModel,
            DagScheduleAssetReference,
            TaskOutletAssetReference,
        )
    except ModuleNotFoundError:
        # dataset is renamed to asset since Airflow 3.0
        from airflow.models.dataset import (
            DagScheduleDatasetReference as DagScheduleAssetReference,
            DatasetDagRunQueue as AssetDagRunQueue,
            DatasetEvent as AssetEvent,
            DatasetModel as AssetModel,
            TaskOutletDatasetReference as TaskOutletAssetReference,
        )

        if AIRFLOW_V_2_10_PLUS:
            from airflow.models.dataset import DatasetAliasModel as AssetAliasModel


def deserialize_operator(serialized_operator: dict[str, Any]) -> Operator:
    if AIRFLOW_V_2_10_PLUS:
        # In airflow 2.10+ we can deserialize operator using regular deserialize method.
        # We do not need to use deserialize_operator method explicitly but some tests are deserializing the
        # operator and in the future they could use regular ``deserialize`` method. This method is a shim
        # to make deserialization of operator works for tests run against older Airflow versions and tests
        # should use that method instead of calling ``BaseSerialization.deserialize`` directly.
        # We can remove this method and switch to the regular ``deserialize`` method as long as all providers
        # are updated to airflow 2.10+.
        from airflow.serialization.serialized_objects import BaseSerialization

        return cast(Operator, BaseSerialization.deserialize(serialized_operator))
    else:
        from airflow.serialization.serialized_objects import SerializedBaseOperator

        return SerializedBaseOperator.deserialize_operator(serialized_operator)


def connection_to_dict(
    connection: Connection, *, prune_empty: bool = False, validate: bool = True
) -> dict[str, Any]:
    """
    Convert Connection to json-serializable dictionary (compatibility code for Airflow 2.7 tests)

    :param connection: connection to convert to dict
    :param prune_empty: Whether or not remove empty values.
    :param validate: Validate dictionary is JSON-serializable

    :meta private:
    """
    conn = {
        "conn_id": connection.conn_id,
        "conn_type": connection.conn_type,
        "description": connection.description,
        "host": connection.host,
        "login": connection.login,
        "password": connection.password,
        "schema": connection.schema,
        "port": connection.port,
    }
    if prune_empty:
        conn = prune_dict(val=conn, mode="strict")
    if (extra := connection.extra_dejson) or not prune_empty:
        conn["extra"] = extra

    if validate:
        json.dumps(conn)
    return conn


def connection_as_json(connection: Connection) -> str:
    """Convert Connection to JSON-string object (compatibility code for Airflow 2.7 tests)."""
    conn_repr = connection_to_dict(connection, prune_empty=True, validate=False)
    conn_repr.pop("conn_id", None)
    return json.dumps(conn_repr)


@contextlib.contextmanager
def ignore_provider_compatibility_error(minimum_version: str, module_name: str):
    """
    Context manager that ignores Provider Compatibility RuntimeError with a specific message.

    :param minimum_version: The version string that should be in the error message.
    :param module_name: The name of the module that is being tested.
    """
    import pytest

    try:
        yield
    except RuntimeError as e:
        if f"needs Apache Airflow {minimum_version}" in str(e):
            pytest.skip(
                reason=f"Skip module {module_name} as "
                f"minimum Airflow version is required {minimum_version}.",
                allow_module_level=True,
            )
        else:
            raise
    except AirflowOptionalProviderFeatureException as e:
        pytest.skip(reason=f"Skip test as optional feature is not available {e}.", allow_module_level=True)
