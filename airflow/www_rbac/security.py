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

from flask_appbuilder.security.sqla import models as sqla_models

###########################################################################
#                               VIEW MENUS
###########################################################################
viewer_vms = [
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
]

user_vms = viewer_vms

op_vms = [
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
]

###########################################################################
#                               PERMISSIONS
###########################################################################

viewer_perms = [
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
]

user_perms = [
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
]

op_perms = [
    'can_conf',
    'can_varimport',
]

###########################################################################
#                     DEFAULT ROLE CONFIGURATIONS
###########################################################################

ROLE_CONFIGS = [
    {
        'role': 'Viewer',
        'perms': viewer_perms,
        'vms': viewer_vms,
    },
    {
        'role': 'User',
        'perms': viewer_perms + user_perms,
        'vms': viewer_vms + user_vms,
    },
    {
        'role': 'Op',
        'perms': viewer_perms + user_perms + op_perms,
        'vms': viewer_vms + user_vms + op_vms,
    },
]


def init_role(sm, role_name, role_vms, role_perms):
    sm_session = sm.get_session
    pvms = sm_session.query(sqla_models.PermissionView).all()
    pvms = [p for p in pvms if p.permission and p.view_menu]

    valid_perms = [p.permission.name for p in pvms]
    valid_vms = [p.view_menu.name for p in pvms]
    invalid_perms = [p for p in role_perms if p not in valid_perms]
    if invalid_perms:
        raise Exception('The following permissions are not valid: {}'
                        .format(invalid_perms))
    invalid_vms = [v for v in role_vms if v not in valid_vms]
    if invalid_vms:
        raise Exception('The following view menus are not valid: {}'
                        .format(invalid_vms))

    role = sm.add_role(role_name)
    role_pvms = []
    for pvm in pvms:
        if pvm.view_menu.name in role_vms and pvm.permission.name in role_perms:
            role_pvms.append(pvm)
    role_pvms = list(set(role_pvms))
    role.permissions = role_pvms
    sm_session.merge(role)
    sm_session.commit()


def init_roles(appbuilder):
    for config in ROLE_CONFIGS:
        name = config['role']
        vms = config['vms']
        perms = config['perms']
        init_role(appbuilder.sm, name, vms, perms)


def is_view_only(user, appbuilder):
    if user.is_anonymous:
        anonymous_role = appbuilder.sm.auth_role_public
        return anonymous_role == 'Viewer'

    user_roles = user.roles
    return len(user_roles) == 1 and user_roles[0].name == 'Viewer'
