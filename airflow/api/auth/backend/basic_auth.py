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
"""
This module is deprecated.

Please use :mod:`airflow.auth.managers.fab.api.auth.backend.basic_auth` instead.
"""
from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Callable

import airflow.auth.managers.fab.api.auth.backend.basic_auth as fab_basic_auth
from airflow.exceptions import RemovedInAirflow3Warning

if TYPE_CHECKING:
    from airflow.auth.managers.fab.models import User

CLIENT_AUTH: tuple[str, str] | Any | None = None

warnings.warn(
    "This module is deprecated. Please use `airflow.auth.managers.fab.api.auth.backend.basic_auth` instead.",
    RemovedInAirflow3Warning,
    stacklevel=2,
)


def init_app(_):
    fab_basic_auth.init_app(_)


def auth_current_user() -> User | None:
    return fab_basic_auth.auth_current_user()


def requires_authentication(function: Callable):
    return fab_basic_auth.requires_authentication(function)
