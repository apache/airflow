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
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.common.compat.sdk import conf
from airflow.providers.fab.www.security.permissions import (
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_CREATE,
    ACTION_CAN_DELETE,
    ACTION_CAN_EDIT,
    ACTION_CAN_READ,
)

if TYPE_CHECKING:
    try:
        from airflow.api_fastapi.auth.managers.base_auth_manager import ExtendedResourceMethod
    except ImportError:
        from airflow.api_fastapi.auth.managers.base_auth_manager import (
            ResourceMethod as ExtendedResourceMethod,
        )
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

# Convert methods to FAB action name
_MAP_METHOD_NAME_TO_FAB_ACTION_NAME: dict[ExtendedResourceMethod, str] = {
    "POST": ACTION_CAN_CREATE,
    "GET": ACTION_CAN_READ,
    "PUT": ACTION_CAN_EDIT,
    "DELETE": ACTION_CAN_DELETE,
    "MENU": ACTION_CAN_ACCESS_MENU,
}
log = logging.getLogger(__name__)


def get_session_lifetime_config():
    """Get session timeout configs and handle outdated configs gracefully."""
    session_lifetime_minutes = conf.get("fab", "session_lifetime_minutes", fallback=None)
    minutes_per_day = 24 * 60
    if not session_lifetime_minutes:
        session_lifetime_days = 30
        session_lifetime_minutes = minutes_per_day * session_lifetime_days

    log.debug("User session lifetime is set to %s minutes.", session_lifetime_minutes)

    return int(session_lifetime_minutes)


def get_fab_auth_manager() -> FabAuthManager:
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

    auth_manager = get_auth_manager()
    if not isinstance(auth_manager, FabAuthManager):
        raise RuntimeError(
            "This functionality is only available with if FabAuthManager is configured as auth manager in the environment."
        )
    return auth_manager


def get_fab_action_from_method_map():
    """Return the map associating a method to a FAB action."""
    return _MAP_METHOD_NAME_TO_FAB_ACTION_NAME


def get_method_from_fab_action_map():
    """Return the map associating a FAB action to a method."""
    return {
        **{v: k for k, v in _MAP_METHOD_NAME_TO_FAB_ACTION_NAME.items()},
    }
