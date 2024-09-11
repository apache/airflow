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

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import BaseAuthManager
    from airflow.www.extensions.init_appbuilder import AirflowAppBuilder

auth_manager: BaseAuthManager | None = None


def get_auth_manager_cls() -> type[BaseAuthManager]:
    """
    Return just the auth manager class without initializing it.

    Useful to save execution time if only static methods need to be called.
    """
    auth_manager_cls = conf.getimport(section="core", key="auth_manager")

    if not auth_manager_cls:
        raise AirflowConfigException(
            "No auth manager defined in the config. "
            "Please specify one using section/key [core/auth_manager]."
        )

    return auth_manager_cls


def init_auth_manager(appbuilder: AirflowAppBuilder) -> BaseAuthManager:
    """
    Initialize the auth manager.

    Import the user manager class and instantiate it.
    """
    global auth_manager
    auth_manager_cls = get_auth_manager_cls()
    auth_manager = auth_manager_cls(appbuilder)
    return auth_manager


def get_auth_manager() -> BaseAuthManager:
    """Return the auth manager, provided it's been initialized before."""
    if auth_manager is None:
        raise RuntimeError(
            "Auth Manager has not been initialized yet. "
            "The `init_auth_manager` method needs to be called first."
        )
    return auth_manager


def is_auth_manager_initialized() -> bool:
    """Return whether the auth manager has been initialized."""
    return auth_manager is not None
