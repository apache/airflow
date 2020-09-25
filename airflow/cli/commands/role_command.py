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
#
"""Roles sub-commands"""
from tabulate import tabulate

from airflow.utils import cli as cli_utils
from airflow.www.app import cached_app


def roles_list(args):
    """Lists all existing roles"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    roles = appbuilder.sm.get_all_roles()
    print("Existing roles:\n")
    role_names = sorted([[r.name] for r in roles])
    msg = tabulate(role_names, headers=['Role'], tablefmt=args.output)
    print(msg)


@cli_utils.action_logging
def roles_create(args):
    """Creates new empty role in DB"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    for role_name in args.role:
        appbuilder.sm.add_role(role_name)


mapping = {
    ("About", "menu_access"): [],
    ("Admin", "menu_access"): [],
    ("Airflow", "can_blocked"): [("Dag", "can_read"), ("DagRun", "can_read")],
    ("Airflow", "can_clear"): [("Dag", "can_read"), ("Task", "can_read"), ("TaskInstance", "can_delete")],
    ("Airflow", "can_code"): [("Dag", "can_read"), ("DagCode", "can_read")],
    ("Airflow", "can_dag_details"): [("Dag", "can_read"), ("DagRun", "can_read")],
    ("Airflow", "can_dag_stats"): [("Dag", "can_read"), ("DagRun", "can_read")],
    ("Airflow", "can_dagrun_clear"): [
        ("Dag", "can_read"),
        ("Task", "can_read"),
        ("TaskInstance", "can_delete"),
    ],
    ("Airflow", "can_dagrun_failed"): [("Dag", "can_read"), ("DagRun", "can_edit")],
    ("Airflow", "can_dagrun_success"): [("Dag", "can_read"), ("DagRun", "can_edit")],
    ("Airflow", "can_delete"): [("Dag", "can_delete")],
    ("Airflow", "can_duration"): [("Dag", "can_read"), ("TaskInstance", "can_read"), ("Task", "can_read")],
    ("Airflow", "can_extra_links"): [("Dag", "can_read"), ("Task", "can_read")],
    ("Airflow", "can_failed"): [("Dag", "can_read"), ("Task", "can_read"), ("TaskInstance", "can_edit")],
    ("Airflow", "can_gantt"): [("Dag", "can_read"), ("Task", "can_read"), ("TaskInstance", "can_read")],
    ("Airflow", "can_get_logs_with_metadata"): [
        ("Dag", "can_read"),
        ("TaskInstance", "can_read"),
        ("Log", "can_read"),
    ],
    ("Airflow", "can_graph"): [
        ("Dag", "can_read"),
        ("TaskInstance", "can_read"),
        ("Task", "can_read"),
        ("Log", "can_read"),
    ],
    ("Airflow", "can_index"): [("Dag", "can_read")],
    ("Airflow", "can_landing_times"): [
        ("Dag", "can_read"),
        ("TaskInstance", "can_read"),
        ("Task", "can_read"),
    ],
    ("Airflow", "can_last_dagruns"): [("Dag", "can_read"), ("DagRun", "can_read")],
    ("Airflow", "can_log"): [("Dag", "can_read"), ("TaskInstance", "can_read"), ("Log", "can_read")],
    ("Airflow", "can_paused"): [("Dag", "can_paused")],
    ("Airflow", "can_redirect_to_external_log"): [
        ("Dag", "can_read"),
        ("TaskInstance", "can_read"),
        ("Log", "can_read"),
    ],
    ("Airflow", "can_refresh"): [("Dag", "can_edit")],
    ("Airflow", "can_refresh_all"): [("Dag", "can_edit")],
    ("Airflow", "can_rendered"): [("Dag", "can_read"), ("TaskInstance", "can_read")],
    ("Airflow", "can_run"): [("Dag", "can_read"), ("Task", "can_read"), ("TaskInstance", "can_create")],
    ("Airflow", "can_success"): [("Dag", "can_read"), ("Task", "can_read"), ("TaskInstance", "can_edit")],
    ("Airflow", "can_task"): [("Dag", "can_read"), ("Task", "can_read"), ("TaskInstance", "can_read")],
    ("Airflow", "can_task_instances"): [
        ("Dag", "can_read"),
        ("Task", "can_read"),
        ("TaskInstance", "can_read"),
    ],
    ("Airflow", "can_task_stats"): [("DagRun", "can_read"), ("TaskInstance", "can_read")],
    ("Airflow", "can_tree"): [
        ("Dag", "can_read"),
        ("TaskInstance", "can_read"),
        ("Task", "can_read"),
        ("Log", "can_read"),
    ],
    ("Airflow", "can_tries"): [("Dag", "can_read"), ("Task", "can_read"), ("TaskInstance", "can_read")],
    ("Airflow", "can_trigger"): [("Dag", "can_read"), ("DagRun", "can_create")],
    ("Airflow", "can_xcom"): [("Dag", "can_read"), ("TaskInstance", "can_read"), ("XCom", "can_read")],
    ("Base Permissions", "menu_access"): [],
    ("Browse", "menu_access"): [],
    ("Config", "can_read"): [],
    ("ConfigurationView", "can_conf"): [],
    ("Configurations", "menu_access"): [],
    ("Connection", "can_create"): [],
    ("Connection", "can_delete"): [],
    ("Connection", "can_edit"): [],
    ("Connection", "can_read"): [],
    ("ConnectionModelView", "can_add"): [("Connection", "can_create")],
    ("ConnectionModelView", "can_delete"): [("Connection", "can_delete")],
    ("ConnectionModelView", "can_edit"): [("Connection", "can_edit")],
    ("ConnectionModelView", "can_list"): [("Connection", "can_read")],
    ("ConnectionModelView", "muldelete"): [("Connection", "can_delete")],
    ("Connections", "menu_access"): [],
    ("DAG Runs", "menu_access"): [],
    ("Dag", "can_edit"): [],
    ("Dag", "can_read"): [],
    ("DagBag", "can_read"): [],
    ("DagCode", "can_read"): [],
    ("DagModelView", "can_list"): [("Dag", "can_read")],
    ("DagModelView", "can_show"): [],
    ("DagRun", "can_create"): [],
    ("DagRun", "can_delete"): [],
    ("DagRun", "can_edit"): [],
    ("DagRun", "can_read"): [],
    ("DagRunModelView", "can_add"): [("DagRun", "can_create")],
    ("DagRunModelView", "can_list"): [("DagRun", "can_read")],
    ("DagRunModelView", "muldelete"): [("DagRun", "can_delete")],
    ("DagRunModelView", "set_failed"): [("DagRun", "can_edit")],
    ("DagRunModelView", "set_running"): [("DagRun", "can_edit")],
    ("DagRunModelView", "set_success"): [("DagRun", "can_edit")],
    ("Docs", "menu_access"): [],
    ("Documentation", "menu_access"): [],
    ("GitHub", "menu_access"): [],
    ("ImportError", "can_read"): [],
    ("JobModelView", "can_list"): [("Job", "can_read")],
    ("Jobs", "menu_access"): [],
    ("List Roles", "menu_access"): [],
    ("List Users", "menu_access"): [],
    ("Log", "can_read"): [],
    ("LogModelView", "can_list"): [("Log", "can_read")],
    ("Logs", "menu_access"): [],
    ("MenuApi", "can_get"): [],
    ("Permission on Views/Menus", "menu_access"): [],
    ("PermissionModelView", "can_list"): [("Permission", "can_read")],
    ("PermissionViewModelView", "can_list"): [("PermissionView", "can_read")],
    ("Pool", "can_create"): [],
    ("Pool", "can_delete"): [],
    ("Pool", "can_edit"): [],
    ("Pool", "can_read"): [],
    ("PoolModelView", "can_add"): [("Pool", "can_create")],
    ("PoolModelView", "can_delete"): [("Pool", "can_delete")],
    ("PoolModelView", "can_edit"): [("Pool", "can_edit")],
    ("PoolModelView", "can_list"): [("Pool", "can_read")],
    ("PoolModelView", "muldelete"): [("Pool", "can_delete")],
    ("Pools", "menu_access"): [],
    ("REST API Reference (Redoc)", "menu_access"): [],
    ("REST API Reference (Swagger UI)", "menu_access"): [],
    ("ResetMyPasswordView", "can_this_form_get"): [],
    ("ResetMyPasswordView", "can_this_form_post"): [],
    ("ResetPasswordView", "can_this_form_get"): [],
    ("ResetPasswordView", "can_this_form_post"): [],
    ("RoleModelView", "can_add"): [("Role", "can_create")],
    ("RoleModelView", "can_delete"): [("Role", "can_delete")],
    ("RoleModelView", "can_download"): [],
    ("RoleModelView", "can_edit"): [("Role", "can_edit")],
    ("RoleModelView", "can_list"): [("Role", "can_read")],
    ("RoleModelView", "can_show"): [],
    ("RoleModelView", "copyrole"): [],
    ("SLA Misses", "menu_access"): [],
    ("Security", "menu_access"): [],
    ("SlaMissModelView", "can_list"): [("SlaMiss", "can_read")],
    ("Task Instances", "menu_access"): [],
    ("Task Reschedules", "menu_access"): [],
    ("Task", "can_edit"): [],
    ("Task", "can_read"): [],
    ("TaskInstance", "can_edit"): [],
    ("TaskInstance", "can_read"): [],
    ("TaskInstanceModelView", "can_list"): [("TaskInstance", "can_read")],
    ("TaskInstanceModelView", "clear"): [],
    ("TaskInstanceModelView", "set_failed"): [("TaskInstance", "can_edit")],
    ("TaskInstanceModelView", "set_retry"): [("TaskInstance", "can_edit")],
    ("TaskInstanceModelView", "set_running"): [("TaskInstance", "can_edit")],
    ("TaskInstanceModelView", "set_success"): [("TaskInstance", "can_edit")],
    ("TaskRescheduleModelView", "can_list"): [("TaskReschedule", "can_read")],
    ("User's Statistics", "menu_access"): [],
    ("UserDBModelView", "can_add"): [("UserDB", "can_create")],
    ("UserDBModelView", "can_delete"): [("UserDB", "can_delete")],
    ("UserDBModelView", "can_download"): [],
    ("UserDBModelView", "can_edit"): [("UserDB", "can_edit")],
    ("UserDBModelView", "can_list"): [("UserDB", "can_read")],
    ("UserDBModelView", "can_show"): [],
    ("UserDBModelView", "can_userinfo"): [],
    ("UserDBModelView", "resetmypassword"): [],
    ("UserDBModelView", "resetpasswords"): [],
    ("UserDBModelView", "userinfoedit"): [],
    ("UserInfoEditView", "can_this_form_get"): [],
    ("UserInfoEditView", "can_this_form_post"): [],
    ("UserStatsChartView", "can_chart"): [],
    ("Variable", "can_create"): [],
    ("Variable", "can_delete"): [],
    ("Variable", "can_edit"): [],
    ("Variable", "can_read"): [],
    ("VariableModelView", "can_add"): [("Variable", "can_create")],
    ("VariableModelView", "can_delete"): [("Variable", "can_delete")],
    ("VariableModelView", "can_edit"): [("Variable", "can_edit")],
    ("VariableModelView", "can_list"): [("Variable", "can_read")],
    ("VariableModelView", "can_varimport"): [],
    ("VariableModelView", "muldelete"): [("Variable", "can_delete")],
    ("VariableModelView", "varexport"): [],
    ("Variables", "menu_access"): [],
    ("Version", "menu_access"): [],
    ("VersionView", "can_version"): [],
    ("ViewMenuModelView", "can_list"): [("ViewMenu", "can_read")],
    ("Views/Menus", "menu_access"): [],
    ("Website", "menu_access"): [],
    ("XCom", "can_read"): [],
    ("XComModelView", "can_delete"): [("XCom", "can_delete")],
    ("XComModelView", "can_list"): [("XCom", "can_read")],
    ("XComModelView", "muldelete"): [("XCom", "can_delete")],
    ("XComs", "menu_access"): [],
    ("all_dags", "can_dag_edit"): [("Dag", "can_edit")],
    ("all_dags", "can_dag_read"): [("Dag", "can_read")],
    ("all_dags", "can_edit"): [("Dag", "can_edit")],
    ("all_dags", "can_read"): [("Dag", "can_read")],
    ("example_bash_operator", "can_edit"): [],
    ("example_bash_operator", "can_read"): [],
    ("example_branch_dop_operator_v3", "can_edit"): [],
    ("example_branch_dop_operator_v3", "can_read"): [],
    ("example_branch_operator", "can_edit"): [],
    ("example_branch_operator", "can_read"): [],
    ("example_complex", "can_edit"): [],
    ("example_complex", "can_read"): [],
    ("example_external_task_marker_child", "can_edit"): [],
    ("example_external_task_marker_child", "can_read"): [],
    ("example_external_task_marker_parent", "can_edit"): [],
    ("example_external_task_marker_parent", "can_read"): [],
    ("example_http_operator", "can_edit"): [],
    ("example_http_operator", "can_read"): [],
    ("example_kubernetes_executor", "can_edit"): [],
    ("example_kubernetes_executor", "can_read"): [],
    ("example_kubernetes_executor_config", "can_edit"): [],
    ("example_kubernetes_executor_config", "can_read"): [],
    ("example_nested_branch_dag", "can_edit"): [],
    ("example_nested_branch_dag", "can_read"): [],
    ("example_passing_params_via_test_command", "can_edit"): [],
    ("example_passing_params_via_test_command", "can_read"): [],
    ("example_pig_operator", "can_edit"): [],
    ("example_pig_operator", "can_read"): [],
    ("example_python_operator", "can_edit"): [],
    ("example_python_operator", "can_read"): [],
    ("example_short_circuit_operator", "can_edit"): [],
    ("example_short_circuit_operator", "can_read"): [],
    ("example_skip_dag", "can_edit"): [],
    ("example_skip_dag", "can_read"): [],
    ("example_subdag_operator", "can_edit"): [],
    ("example_subdag_operator", "can_read"): [],
    ("example_subdag_operator.section-1", "can_edit"): [],
    ("example_subdag_operator.section-1", "can_read"): [],
    ("example_subdag_operator.section-2", "can_edit"): [],
    ("example_subdag_operator.section-2", "can_read"): [],
    ("example_trigger_controller_dag", "can_edit"): [],
    ("example_trigger_controller_dag", "can_read"): [],
    ("example_trigger_target_dag", "can_edit"): [],
    ("example_trigger_target_dag", "can_read"): [],
    ("example_xcom", "can_edit"): [],
    ("example_xcom", "can_read"): [],
    ("example_xcom_args", "can_edit"): [],
    ("example_xcom_args", "can_read"): [],
    ("latest_only", "can_edit"): [],
    ("latest_only", "can_read"): [],
    ("latest_only_with_trigger", "can_edit"): [],
    ("latest_only_with_trigger", "can_read"): [],
    ("test_nested_subdags", "can_edit"): [],
    ("test_nested_subdags", "can_read"): [],
    ("test_nested_subdags.level1", "can_edit"): [],
    ("test_nested_subdags.level1", "can_read"): [],
    ("test_nested_subdags.level1.level2", "can_edit"): [],
    ("test_nested_subdags.level1.level2", "can_read"): [],
    ("test_utils", "can_edit"): [],
    ("test_utils", "can_read"): [],
    ("tutorial", "can_edit"): [],
    ("tutorial", "can_read"): [],
}


# @cli_utils.action_logging
def roles_upgrade(args):
    """Creates new empty role in DB"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    roles = appbuilder.sm.get_all_roles()
    # breakpoint()
    permissions = set()
    for role in roles:
        #         print(f"""

        # ===============================
        # {role.name}
        # ===============================""")
        #         permissions = [f"{p.view_menu}.{p.permission}" for p in role.permissions]
        # for permission in sorted(permissions):
        for permission in role.permissions:
            # breakpoint()
            # if p.view_menu.name.endswith("ModelView"):
            #     resource = p.view_menu.name.replace("ModelView", "")
            #     actions = {
            #         "can_add": "can_create",
            #         "can_list": "can_read",
            #         "can_edit": "can_edit",
            #         "can_delete": "can_delete",
            #         "muldelete": "can_delete",
            #         "set_failed": "can_edit",
            #         "set_retry": "can_edit",
            #         "set_running": "can_edit",
            #         "set_success": "can_edit",
            #     }
            #     action = actions.get(p.permission.name)
            #     if action:
            #         perm = f'("{p.view_menu}", "{p.permission}"): [("{resource}", "{action}")],'
            #         permissions.add(perm)
            #         continue

            perm = f'("{permission.view_menu}", "{permission.permission}"): [],'
            permissions.add(perm)
            # print(f"{permission} -----> ")
            # print(f"{permission.view_menu}.{permission.permission}")
    # msg = tabulate(role_names,
    #                headers=['Role'],
    #                tablefmt=None)
    # print(msg)
    for perm in sorted(list(permissions)):
        print(perm)


if __name__ == '__main__':
    roles_upgrade({"output": None})
