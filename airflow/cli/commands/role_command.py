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
    # ========================= Airflow Views
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
    # =========================
    ("Config", "can_read"): [("Config", "can_read")],  # config_endpoint
    ("ConfigurationView", "can_conf"): [],  # views.py
    ("ConnectionModelView", "can_add"): [("Connection", "can_create")],  # views.py
    ("ConnectionModelView", "can_delete"): [("Connection", "can_delete")],  # views.py
    ("ConnectionModelView", "can_edit"): [("Connection", "can_edit")],  # views.py
    ("ConnectionModelView", "can_list"): [("Connection", "can_read")],  # views.py
    ("ConnectionModelView", "muldelete"): [("Connection", "can_delete")],  # views.py
    ("DagModelView", "can_list"): [("Dag", "can_read")],  # views.py
    ("DagModelView", "can_show"): [("Dag", "can_read")],  # views.py
    ("DagRunModelView", "can_add"): [("DagRun", "can_create")],  # views.py
    ("DagRunModelView", "can_list"): [("DagRun", "can_read")],  # views.py
    ("DagRunModelView", "muldelete"): [("DagRun", "can_delete")],  # views.py
    ("DagRunModelView", "set_failed"): [("DagRun", "can_edit")],  # views.py
    ("DagRunModelView", "set_running"): [("DagRun", "can_edit")],  # views.py
    ("DagRunModelView", "set_success"): [("DagRun", "can_edit")],  # views.py
    ("ImportError", "can_read"): [],  # import_error_endpoint
    ("JobModelView", "can_list"): [("Job", "can_read")],  # views.py
    ("Log", "can_read"): [],  # event_log_endpoint.py
    ("LogModelView", "can_list"): [("Log", "can_read")],  # views.py
    ("MenuApi", "can_get"): [],  # FAB menu.py
    ("PermissionModelView", "can_list"): [("Permission", "can_read")],  # FAB
    ("PermissionViewModelView", "can_list"): [("PermissionView", "can_read")],  # FAB
    ("PoolModelView", "can_add"): [("Pool", "can_create")],  # views.py
    ("PoolModelView", "can_delete"): [("Pool", "can_delete")],  # views.py
    ("PoolModelView", "can_edit"): [("Pool", "can_edit")],  # views.py
    ("PoolModelView", "can_list"): [("Pool", "can_read")],  # views.py
    ("PoolModelView", "muldelete"): [("Pool", "can_delete")],  # views.py
    ("ResetMyPasswordView", "can_this_form_get"): [],  # FAB
    ("ResetMyPasswordView", "can_this_form_post"): [],  # FAB
    ("ResetPasswordView", "can_this_form_get"): [],  # FAB
    ("ResetPasswordView", "can_this_form_post"): [],  # FAB
    ("RoleModelView", "can_add"): [("Role", "can_create")],  # FAB
    ("RoleModelView", "can_delete"): [("Role", "can_delete")],  # FAB
    ("RoleModelView", "can_download"): [("Role", "can_read")],  # FAB
    ("RoleModelView", "can_edit"): [("Role", "can_edit")],  # FAB
    ("RoleModelView", "can_list"): [("Role", "can_read")],  # FAB
    ("RoleModelView", "can_show"): [("Role", "can_read")],  # FAB
    ("RoleModelView", "copyrole"): [("Role", "can_create")],  # FAB
    ("SlaMissModelView", "can_list"): [("SlaMiss", "can_read")],  # views.py
    ("Task", "can_edit"): [("Task", "can_edit")],  # task_endpoint.py
    ("Task", "can_read"): [("Task", "can_read")],  # task_endpoint.py
    ("TaskInstanceModelView", "can_list"): [("TaskInstance", "can_read")],  # views.py
    ("TaskInstanceModelView", "clear"): [],  # views.py
    ("TaskInstanceModelView", "set_failed"): [("TaskInstance", "can_edit")],  # views.py
    ("TaskInstanceModelView", "set_retry"): [("TaskInstance", "can_edit")],  # views.py
    ("TaskInstanceModelView", "set_running"): [("TaskInstance", "can_edit")],  # views.py
    ("TaskInstanceModelView", "set_success"): [("TaskInstance", "can_edit")],  # views.py
    ("TaskRescheduleModelView", "can_list"): [("TaskReschedule", "can_read")],  # views.py
    ("UserDBModelView", "can_add"): [("UserDB", "can_create")],  # FAB
    ("UserDBModelView", "can_delete"): [("UserDB", "can_delete")],  # FAB
    ("UserDBModelView", "can_download"): [],  # FAB
    ("UserDBModelView", "can_edit"): [("UserDB", "can_edit")],  # FAB
    ("UserDBModelView", "can_list"): [("UserDB", "can_read")],  # FAB
    ("UserDBModelView", "can_show"): [],  # FAB
    ("UserDBModelView", "can_userinfo"): [],  # FAB
    ("UserDBModelView", "resetmypassword"): [],  # FAB
    ("UserDBModelView", "resetpasswords"): [],  # FAB
    ("UserDBModelView", "userinfoedit"): [],  # FAB
    ("UserInfoEditView", "can_this_form_get"): [],  # FAB
    ("UserInfoEditView", "can_this_form_post"): [],  # FAB
    ("UserStatsChartView", "can_chart"): [],  # FAB
    ("VariableModelView", "can_add"): [("Variable", "can_create")],  # views.py
    ("VariableModelView", "can_delete"): [("Variable", "can_delete")],  # views.py
    ("VariableModelView", "can_edit"): [("Variable", "can_edit")],  # views.py
    ("VariableModelView", "can_list"): [("Variable", "can_read")],  # views.py
    ("VariableModelView", "can_varimport"): [],  # views.py
    ("VariableModelView", "muldelete"): [("Variable", "can_delete")],  # views.py
    ("VariableModelView", "varexport"): [],  # views.py
    ("VersionView", "can_version"): [],  # views.py
    ("ViewMenuModelView", "can_list"): [("ViewMenu", "can_read")],  # FAB
    ("XComModelView", "can_delete"): [("XCom", "can_delete")],  # views.py
    ("XComModelView", "can_list"): [("XCom", "can_read")],  # views.py
    ("XComModelView", "muldelete"): [("XCom", "can_delete")],  # views.py
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
