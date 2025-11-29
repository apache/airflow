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
AIRFLOW_V_3_1_PLUS: bool = get_base_airflow_version_tuple() >= (3, 1, 0)
AIRFLOW_V_3_1_1_PLUS: bool = get_base_airflow_version_tuple() >= (3, 1, 1)

try:
    from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
except ImportError:
    from airflow.utils.types import NOTSET, ArgNotSet  # type: ignore[attr-defined,no-redef]
try:
    from airflow.sdk.definitions._internal.types import is_arg_set
except ImportError:

    def is_arg_set(value):  # type: ignore[misc,no-redef]
        return value is not NOTSET


__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "AIRFLOW_V_3_1_PLUS",
    "AIRFLOW_V_3_1_1_PLUS",
    "NOTSET",
    "ArgNotSet",
    "is_arg_set",
]
