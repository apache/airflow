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

from importlib.metadata import version
from typing import TYPE_CHECKING, Any, cast

from packaging.version import Version

from airflow.models import Operator

try:
    # ImportError has been renamed to ParseImportError in airflow 2.10.0, and since our provider tests should
    # run on all supported versions of Airflow, this compatibility shim falls back to the old ImportError so
    # that tests can import it from here and use it in their code and run against older versions of Airflow
    # This import can be removed (and all tests switched to import ParseImportError directly) as soon as
    # all providers are updated to airflow 2.10+.
    from airflow.models.errors import ParseImportError
except ImportError:
    from airflow.models.errors import ImportError as ParseImportError  # type: ignore[no-redef]

from airflow import __version__ as airflow_version

AIRFLOW_VERSION = Version(airflow_version)
AIRFLOW_V_2_7_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("2.7.0")
AIRFLOW_V_2_8_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("2.8.0")
AIRFLOW_V_2_9_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("2.9.0")
AIRFLOW_V_2_10_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("2.10.0")


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
