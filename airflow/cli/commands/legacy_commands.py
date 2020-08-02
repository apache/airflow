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

from argparse import ArgumentError


def check_value(action, value):  # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    """ Checks command value and raise error if value is in removed command"""

    # celery group
    if value == "worker":
        message = "worker command has been removed, please use `airflow celery worker`"
        raise ArgumentError(action, message)
    if value == "flower":
        message = "flower command has been removed, please use `airflow celery flower`"
        raise ArgumentError(action, message)

    # dags group
    if value == "trigger_dag":
        message = "trigger_dag command has been removed, please use `airflow dags trigger`"
        raise ArgumentError(action, message)
    if value == "delete_dag":
        message = "delete_dag command has been removed, please use `airflow dags delete`"
        raise ArgumentError(action, message)
    if value == "show_dag":
        message = "show_dag command has been removed, please use `airflow dags show`"
        raise ArgumentError(action, message)
    if value == "list_dag":
        message = "list_dag command has been removed, please use `airflow dags list`"
        raise ArgumentError(action, message)
    if value == "dag_status":
        message = "dag_status command has been removed, please use `airflow dags status`"
        raise ArgumentError(action, message)
    if value == "backfill":
        message = "backfill command has been removed, please use `airflow dags backfill`"
        raise ArgumentError(action, message)
    if value == "list_dag_runs":
        message = "list_dag_runs command has been removed, please use `airflow dags list_runs`"
        raise ArgumentError(action, message)
    if value == "pause":
        message = "pause command has been removed, please use `airflow dags pause`"
        raise ArgumentError(action, message)
    if value == "unpause":
        message = "unpause command has been removed, please use `airflow dags unpause`"
        raise ArgumentError(action, message)

    # tasks group
    if value == "test":
        message = "test command has been removed, please use `airflow tasks test`"
        raise ArgumentError(action, message)
    if value == "clear":
        message = "clear command has been removed, please use `airflow tasks clear`"
        raise ArgumentError(action, message)
    if value == "list_tasks":
        message = "list_tasks command has been removed, please use `airflow tasks list`"
        raise ArgumentError(action, message)
    if value == "task_failed_deps":
        message = "task_failed_deps command has been removed, please use `airflow tasks failed_deps`"
        raise ArgumentError(action, message)
    if value == "task_state":
        message = "task_state command has been removed, please use `airflow tasks state`"
        raise ArgumentError(action, message)
    if value == "run":
        message = "run command has been removed, please use `airflow tasks run`"
        raise ArgumentError(action, message)
    if value == "render":
        message = "render command has been removed, please use `airflow tasks render`"
        raise ArgumentError(action, message)

    # db group
    if value == "initdb":
        message = "initdb command has been removed, please use `airflow db init`"
        raise ArgumentError(action, message)
    if value == "resetdb":
        message = "resetdb command has been removed, please use `airflow db reset`"
        raise ArgumentError(action, message)
    if value == "upgradedb":
        message = "upgradedb command has been removed, please use `airflow db upgrade`"
        raise ArgumentError(action, message)
    if value == "checkdb":
        message = "checkdb command has been removed, please use `airflow db check`"
        raise ArgumentError(action, message)
    if value == "shell":
        message = "shell command has been removed, please use `airflow db shell`"
        raise ArgumentError(action, message)

    # pools group
    if value == "pool":
        message = "pool command has been removed, please use `airflow pools`"
        raise ArgumentError(action, message)

    # users group
    if value == "list_users":
        message = "list_users command has been removed, please use `airflow users list`"
        raise ArgumentError(action, message)

    if value == "create_user":
        message = "create_user command has been removed, please use `airflow users create`"
        raise ArgumentError(action, message)

    if value == "delete_user":
        message = "delete_user command has been removed, please use `airflow users delete`"
        raise ArgumentError(action, message)
