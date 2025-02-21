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

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

AVP_PREFIX_ENTITIES = "Airflow::"


class AvpEntities(Enum):
    """Enum of Amazon Verified Permissions entities."""

    ACTION = "Action"
    GROUP = "Group"
    USER = "User"

    # Resource types
    ASSET = "Asset"
    CONFIGURATION = "Configuration"
    CONNECTION = "Connection"
    CUSTOM = "Custom"
    DAG = "Dag"
    MENU = "Menu"
    POOL = "Pool"
    VARIABLE = "Variable"
    VIEW = "View"


def get_entity_type(resource_type: AvpEntities) -> str:
    """
    Return entity type.

    :param resource_type: Resource type.

    Example: Airflow::Action, Airflow::Group, Airflow::Variable, Airflow::User.
    """
    return AVP_PREFIX_ENTITIES + resource_type.value


def get_action_id(resource_type: AvpEntities, method: ResourceMethod | str):
    """
    Return action id.

    Convention for action ID is <resource_type>.<method>. Example: Variable.GET.

    :param resource_type: Resource type.
    :param method: Resource method.
    """
    return f"{resource_type.value}.{method}"
