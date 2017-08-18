# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from flask import current_app
from flask_appbuilder.security.sqla import models as sqla_models
from airflow import configuration as conf

from app import appbuilder
sm = appbuilder.sm

read_only_perms = [
    'menu_access',
    'can_index',
    'can_list',
    'can_show',
    'can_query',
    'can_chart',
    'can_dag_stats',
    'can_dag_details',
    'can_task_stats',
    'can_code',
    'can_log',
    'can_tries',
    'can_graph',
    'can_tree',
    'can_task',
    'can_task_instances',
    'can_xcom',
    'can_gantt',
    'can_landing_times',
    'can_duration',
    'can_refresh',
    'can_blocked',
    'can_conf',
    'can_version',
]

read_only_vms = [
    'Airflow',
    'DagModelModelView',

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

    'Admin',
    'Configurations',
    'XComs',
    'XComModelView',
    'Connections',
    'ConnectionModelView',
    'Variables',
    'VariableModelView',
    'Pools',
    'PoolModelView',

    'Data Profiling',
    'Ad Hoc Query',
    'QueryView',
    'Known Events',
    'KnownEventModelView',
    'Charts',
    'ChartModelView',

    'Docs',
    'Documentation',
    'Github',

    'About',
    'Version',
]


def is_read_only_pvm(pvm):
    return (pvm.view_menu.name in read_only_vms and
            pvm.permission.name in read_only_perms)


def init_role(role_name, pvm_check):
    session = sm.get_session()
    pvms = session.query(sqla_models.PermissionView).all()
    pvms = [p for p in pvms if p.permission and p.view_menu]
    role = sm.add_role(role_name)
    role_pvms = [p for p in pvms if pvm_check(p)]
    role.permissions = role_pvms
    session.merge(role)
    session.commit()

def init_roles():
    init_role('Read Only', is_read_only_pvm)

init_roles()