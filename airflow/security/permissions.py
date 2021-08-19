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
from flask_babel import gettext

# Resource Constants
RESOURCE_ADMIN_MENU = gettext("Admin")
RESOURCE_AIRFLOW = gettext("Airflow")
RESOURCE_AUDIT_LOG = gettext("Audit Logs")
RESOURCE_BROWSE_MENU = gettext("Browse")
RESOURCE_DAG = gettext("DAGs")
RESOURCE_DAG_PREFIX = gettext("DAG:")
RESOURCE_LOGIN = gettext("Logins")
RESOURCE_DOCS_MENU = gettext("Docs")
RESOURCE_DOCS = gettext("Documentation")
RESOURCE_CONFIG = gettext("Configurations")
RESOURCE_CONNECTION = gettext("Connections")
RESOURCE_DAG_DEPENDENCIES = gettext("DAG Dependencies")
RESOURCE_DAG_CODE = gettext("DAG Code")
RESOURCE_DAG_RUN = gettext("DAG Runs")
RESOURCE_IMPORT_ERROR = gettext("ImportError")
RESOURCE_JOB = gettext("Jobs")
RESOURCE_MY_PASSWORD = gettext("My Password")
RESOURCE_MY_PROFILE = gettext("My Profile")
RESOURCE_PASSWORD = gettext("Passwords")
RESOURCE_PERMISSION = gettext("Permissions")
RESOURCE_PERMISSION_VIEW = gettext("Permission Views")  # Refers to a Perm <-> View mapping, not an MVC View.
RESOURCE_POOL = gettext("Pools")
RESOURCE_PLUGIN = gettext("Plugins")
RESOURCE_PROVIDER = gettext("Providers")
RESOURCE_ROLE = gettext("Roles")
RESOURCE_SLA_MISS = gettext("SLA Misses")
RESOURCE_TASK_INSTANCE = gettext("Task Instances")
RESOURCE_TASK_LOG = gettext("Task Logs")
RESOURCE_TASK_RESCHEDULE = gettext("Task Reschedules")
RESOURCE_USER = gettext("Users")
RESOURCE_USER_STATS_CHART = gettext("User Stats Chart")
RESOURCE_VARIABLE = gettext("Variables")
RESOURCE_VIEW_MENU = gettext("View Menus")
RESOURCE_WEBSITE = gettext("Website")
RESOURCE_XCOM = gettext("XComs")

# curve resources
RESOURCE_CURVE_TEMPLATE = gettext('Curve Template')
RESOURCE_CURVE = gettext('Curve')
RESOURCE_CURVES = gettext('Curves')
RESOURCE_RESULT = gettext('Results')
RESOURCE_ERROR_TAG = gettext('Error Tag')
RESOURCE_CONTROLLER = gettext('Equipments')
RESOURCE_DEVICE_TYPE = gettext('Device Type')
RESOURCE_ANALYSIS_VIA_TRACK_NO = gettext('Analysis Via Track No')
RESOURCE_ANALYSIS_VIA_CONTROLLER = gettext('Analysis Via Controller')
RESOURCE_ANALYSIS_VIA_BOLT_NO = gettext('Analysis Via Bolt No')

# curve menus
RESOURCE_MASTER_DATA_MANAGEMENT = gettext('Master Data Management')
RESOURCE_ANALYSIS = gettext('Analysis')


# Action Constants
ACTION_CAN_CREATE = "can_create"
ACTION_CAN_READ = "can_read"
ACTION_CAN_EDIT = "can_edit"
ACTION_CAN_DELETE = "can_delete"
ACTION_CAN_ACCESS_MENU = "menu_access"
DEPRECATED_ACTION_CAN_DAG_READ = "can_dag_read"
DEPRECATED_ACTION_CAN_DAG_EDIT = "can_dag_edit"

DAG_PERMS = {ACTION_CAN_READ, ACTION_CAN_EDIT}


def resource_name_for_dag(dag_id):
    """Returns the resource name for a DAG id."""
    if dag_id == RESOURCE_DAG:
        return dag_id

    if dag_id.startswith(RESOURCE_DAG_PREFIX):
        return dag_id
    return f"{RESOURCE_DAG_PREFIX}{dag_id}"
