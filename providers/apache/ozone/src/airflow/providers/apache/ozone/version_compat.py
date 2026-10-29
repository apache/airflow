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
#
# NOTE! THIS FILE IS COPIED MANUALLY IN OTHER PROVIDERS DELIBERATELY TO AVOID ADDING UNNECESSARY
# DEPENDENCIES BETWEEN PROVIDERS. IF YOU WANT TO ADD CONDITIONAL CODE IN YOUR PROVIDER THAT DEPENDS
# ON AIRFLOW VERSION, PLEASE COPY THIS FILE TO THE ROOT PACKAGE OF YOUR PROVIDER AND IMPORT
# THOSE CONSTANTS FROM IT RATHER THAN IMPORTING THEM FROM ANOTHER PROVIDER OR TEST CODE
#

from __future__ import annotations

import functools


@functools.cache
def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    from packaging.version import Version

    from airflow import __version__

    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


AIRFLOW_V_3_0_PLUS = get_base_airflow_version_tuple() >= (3, 0, 0)

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import Connection, timezone
    from airflow.sdk._shared.secrets_masker import mask_secret, redact
    from airflow.sdk.bases.hook import BaseHook
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.bases.sensor import BaseSensorOperator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.exceptions import AirflowException
else:
    from airflow.exceptions import AirflowException
    from airflow.hooks.base import BaseHook  # type: ignore[attr-defined,no-redef]
    from airflow.models import BaseOperator
    from airflow.models.connection import Connection  # type: ignore[assignment]
    from airflow.sensors.base import BaseSensorOperator  # type: ignore[no-redef]
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef] # noqa: TID251
    from airflow.utils.context import Context  # type: ignore[attr-defined,no-redef]
    from airflow.utils.log.secrets_masker import mask_secret, redact  # type: ignore[no-redef]

__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "AirflowException",
    "BaseHook",
    "BaseOperator",
    "BaseSensorOperator",
    "Connection",
    "Context",
    "get_base_airflow_version_tuple",
    "mask_secret",
    "redact",
    "timezone",
]
