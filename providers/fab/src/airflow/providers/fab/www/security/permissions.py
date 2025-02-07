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

from typing import TypedDict

# Resource Constants
RESOURCE_ACTION = "Permissions"
RESOURCE_ADMIN_MENU = "Admin"
RESOURCE_AUDIT_LOG = "Audit Logs"
RESOURCE_BROWSE_MENU = "Browse"
RESOURCE_CONFIG = "Configurations"
RESOURCE_CONNECTION = "Connections"
RESOURCE_DAG = "DAGs"
RESOURCE_DAG_CODE = "DAG Code"
RESOURCE_DAG_DEPENDENCIES = "DAG Dependencies"
RESOURCE_DAG_PREFIX = "DAG:"
RESOURCE_DAG_RUN = "DAG Runs"
RESOURCE_DAG_RUN_PREFIX = "DAG Run:"
RESOURCE_DAG_WARNING = "DAG Warnings"
RESOURCE_CLUSTER_ACTIVITY = "Cluster Activity"
RESOURCE_ASSET = "Assets"
RESOURCE_DOCS = "Documentation"
RESOURCE_DOCS_MENU = "Docs"
RESOURCE_IMPORT_ERROR = "ImportError"
RESOURCE_JOB = "Jobs"
RESOURCE_MY_PASSWORD = "My Password"
RESOURCE_MY_PROFILE = "My Profile"
RESOURCE_PASSWORD = "Passwords"
RESOURCE_PERMISSION = "Permission Views"  # Refers to a Perm <-> View mapping, not an MVC View.
RESOURCE_PLUGIN = "Plugins"
RESOURCE_POOL = "Pools"
RESOURCE_PROVIDER = "Providers"
RESOURCE_RESOURCE = "View Menus"
RESOURCE_ROLE = "Roles"
RESOURCE_SLA_MISS = "SLA Misses"
RESOURCE_TASK_INSTANCE = "Task Instances"
RESOURCE_TASK_LOG = "Task Logs"
RESOURCE_TASK_RESCHEDULE = "Task Reschedules"
RESOURCE_TRIGGER = "Triggers"
RESOURCE_USER = "Users"
RESOURCE_USER_STATS_CHART = "User Stats Chart"
RESOURCE_VARIABLE = "Variables"
RESOURCE_WEBSITE = "Website"
RESOURCE_XCOM = "XComs"

# Action Constants
ACTION_CAN_CREATE = "can_create"
ACTION_CAN_READ = "can_read"
ACTION_CAN_EDIT = "can_edit"
ACTION_CAN_DELETE = "can_delete"
ACTION_CAN_ACCESS_MENU = "menu_access"
DEPRECATED_ACTION_CAN_DAG_READ = "can_dag_read"
DEPRECATED_ACTION_CAN_DAG_EDIT = "can_dag_edit"


class ResourceDetails(TypedDict):
    """Details of a resource (actions and prefix)."""

    actions: set[str]
    prefix: str


# Keeping DAG_ACTIONS to keep the compatibility with outdated versions of FAB provider
DAG_ACTIONS = {ACTION_CAN_READ, ACTION_CAN_EDIT, ACTION_CAN_DELETE}

RESOURCE_DETAILS_MAP = {
    RESOURCE_DAG: ResourceDetails(
        actions={ACTION_CAN_READ, ACTION_CAN_EDIT, ACTION_CAN_DELETE}, prefix=RESOURCE_DAG_PREFIX
    ),
    RESOURCE_DAG_RUN: ResourceDetails(
        actions={ACTION_CAN_READ, ACTION_CAN_CREATE, ACTION_CAN_DELETE, ACTION_CAN_ACCESS_MENU},
        prefix=RESOURCE_DAG_RUN_PREFIX,
    ),
}
PREFIX_LIST = [details["prefix"] for details in RESOURCE_DETAILS_MAP.values()]
PREFIX_RESOURCES_MAP = {details["prefix"]: resource for resource, details in RESOURCE_DETAILS_MAP.items()}


def resource_name(root_dag_id: str, resource: str) -> str:
    """
    Return the resource name for a DAG id.

    Note that since a sub-DAG should follow the permission of its
    parent DAG, you should pass ``DagModel.root_dag_id`` to this function,
    for a subdag. A normal dag should pass the ``DagModel.dag_id``.
    """
    if root_dag_id in RESOURCE_DETAILS_MAP.keys():
        return root_dag_id
    if root_dag_id.startswith(tuple(PREFIX_RESOURCES_MAP.keys())):
        return root_dag_id
    return f"{RESOURCE_DETAILS_MAP[resource]['prefix']}{root_dag_id}"


def resource_name_for_dag(root_dag_id: str) -> str:
    """
    Return the resource name for a DAG id.

    Note that since a sub-DAG should follow the permission of its
    parent DAG, you should pass ``DagModel.root_dag_id`` to this function,
    for a subdag. A normal dag should pass the ``DagModel.dag_id``.

    Note: This function is kept for backwards compatibility.
    """
    if root_dag_id == RESOURCE_DAG:
        return root_dag_id
    if root_dag_id.startswith(RESOURCE_DAG_PREFIX):
        return root_dag_id
    return f"{RESOURCE_DAG_PREFIX}{root_dag_id}"
