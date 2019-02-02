# -*- coding: utf-8 -*-
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

###########################################################################
#                               VIEW MENUS
###########################################################################
VIEWER_VMS = {
    'Airflow',
    'DagModelView',
    'Browse',
    'DAG Runs',
    'DagRunModelView',
    'Task Instances',
    'TaskInstanceModelView',
    'SLA Misses',
    'SlaMissModelView',
    'Jobs',
    'JobModelView',
    'Logs',
    'LogModelView',
    'Docs',
    'Documentation',
    'Github',
    'About',
    'Version',
    'VersionView',
}

USER_VMS = VIEWER_VMS

OP_VMS = {
    'Admin',
    'Configurations',
    'ConfigurationView',
    'Connections',
    'ConnectionModelView',
    'Pools',
    'PoolModelView',
    'Variables',
    'VariableModelView',
    'XComs',
    'XComModelView',
}

###########################################################################
#                               PERMISSIONS
###########################################################################

VIEWER_PERMS = {
    'menu_access',
    'can_index',
    'can_list',
    'can_show',
    'can_chart',
    'can_dag_stats',
    'can_dag_details',
    'can_task_stats',
    'can_code',
    'can_log',
    'can_get_logs_with_metadata',
    'can_tries',
    'can_graph',
    'can_tree',
    'can_task',
    'can_task_instances',
    'can_xcom',
    'can_gantt',
    'can_landing_times',
    'can_duration',
    'can_blocked',
    'can_rendered',
    'can_pickle_info',
    'can_version',
}

USER_PERMS = {
    'can_dagrun_clear',
    'can_run',
    'can_trigger',
    'can_add',
    'can_edit',
    'can_delete',
    'can_paused',
    'can_refresh',
    'can_success',
    'muldelete',
    'set_failed',
    'set_running',
    'set_success',
    'clear',
    'can_clear',
}

OP_PERMS = {
    'can_conf',
    'can_varimport',
}

# global view-menu for dag-level access
DAG_VMS = {
    'all_dags'
}

DAG_PERMS = {
    'can_dag_read',
    'can_dag_edit',
}

###########################################################################
#                     DEFAULT ROLE CONFIGURATIONS
###########################################################################

ROLE_CONFIGS = [
    {
        'role': 'Viewer',
        'perms': VIEWER_PERMS | DAG_PERMS,
        'vms': VIEWER_VMS | DAG_VMS
    },
    {
        'role': 'User',
        'perms': VIEWER_PERMS | USER_PERMS | DAG_PERMS,
        'vms': VIEWER_VMS | DAG_VMS | USER_VMS,
    },
    {
        'role': 'Op',
        'perms': VIEWER_PERMS | USER_PERMS | OP_PERMS | DAG_PERMS,
        'vms': VIEWER_VMS | DAG_VMS | USER_VMS | OP_VMS,
    },
]

EXISTING_ROLES = {
    'Admin',
    'Viewer',
    'User',
    'Op',
    'Public',
}
