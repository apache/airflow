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

from typing import TYPE_CHECKING

from airflow.compat.functools import cache
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import BaseAuthManager


@cache
def get_auth_manager() -> BaseAuthManager:
    """
    Initialize auth manager.

    Import the user manager class, instantiate it and return it.
    """
    auth_manager_cls = conf.getimport(section="core", key="auth_manager")

    if not auth_manager_cls:
        raise AirflowConfigException(
            "No auth manager defined in the config. "
            "Please specify one using section/key [core/auth_manager]."
        )

    return auth_manager_cls()
