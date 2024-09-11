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

from typing import TYPE_CHECKING

from airflow.security.permissions import (
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_CREATE,
    ACTION_CAN_DELETE,
    ACTION_CAN_EDIT,
    ACTION_CAN_READ,
)

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

# Convert methods to FAB action name
_MAP_METHOD_NAME_TO_FAB_ACTION_NAME: dict[ResourceMethod, str] = {
    "POST": ACTION_CAN_CREATE,
    "GET": ACTION_CAN_READ,
    "PUT": ACTION_CAN_EDIT,
    "DELETE": ACTION_CAN_DELETE,
    "MENU": ACTION_CAN_ACCESS_MENU,
}


def get_fab_action_from_method_map():
    """Return the map associating a method to a FAB action."""
    return _MAP_METHOD_NAME_TO_FAB_ACTION_NAME


def get_method_from_fab_action_map():
    """Return the map associating a FAB action to a method."""
    return {
        **{v: k for k, v in _MAP_METHOD_NAME_TO_FAB_ACTION_NAME.items()},
    }
