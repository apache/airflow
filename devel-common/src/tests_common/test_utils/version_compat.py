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


def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    from packaging.version import Version

    from airflow import __version__

    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


AIRFLOW_V_3_0_1 = get_base_airflow_version_tuple() == (3, 0, 1)
AIRFLOW_V_3_0_PLUS = get_base_airflow_version_tuple() >= (3, 0, 0)
AIRFLOW_V_3_0_3_PLUS = get_base_airflow_version_tuple() >= (3, 0, 3)
AIRFLOW_V_3_1_PLUS = get_base_airflow_version_tuple() >= (3, 1, 0)
AIRFLOW_V_3_1_3_PLUS = get_base_airflow_version_tuple() >= (3, 1, 3)
AIRFLOW_V_3_2_PLUS = get_base_airflow_version_tuple() >= (3, 2, 0)

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import PokeReturnValue, timezone
    from airflow.sdk.bases.xcom import BaseXCom
    from airflow.sdk.definitions._internal.decorators import remove_task_decorator
    from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet

    XCOM_RETURN_KEY = BaseXCom.XCOM_RETURN_KEY
else:
    from airflow.sensors.base import PokeReturnValue  # type: ignore[no-redef]
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]
    from airflow.utils.decorators import remove_task_decorator  # type: ignore[no-redef]
    from airflow.utils.types import NOTSET, ArgNotSet  # type: ignore[attr-defined,no-redef]
    from airflow.utils.xcom import XCOM_RETURN_KEY  # type: ignore[no-redef]


def get_sqlalchemy_version_tuple() -> tuple[int, int, int]:
    import sqlalchemy
    from packaging.version import Version

    sqlalchemy_version = Version(sqlalchemy.__version__)
    return sqlalchemy_version.major, sqlalchemy_version.minor, sqlalchemy_version.micro


SQLALCHEMY_V_1_4 = (1, 4, 0) <= get_sqlalchemy_version_tuple() < (2, 0, 0)
SQLALCHEMY_V_2_0 = (2, 0, 0) <= get_sqlalchemy_version_tuple() < (2, 1, 0)

__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "AIRFLOW_V_3_0_1",
    "AIRFLOW_V_3_1_PLUS",
    "AIRFLOW_V_3_2_PLUS",
    "NOTSET",
    "SQLALCHEMY_V_1_4",
    "SQLALCHEMY_V_2_0",
    "XCOM_RETURN_KEY",
    "ArgNotSet",
    "PokeReturnValue",
    "remove_task_decorator",
    "timezone",
]
