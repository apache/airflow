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
"""Example DAG demonstrating HBase CLI execute_command method.

This DAG shows how to use the universal execute_command method to run
arbitrary HBase CLI commands for backup set management.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.providers.apache.hbase.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG
else:
    from airflow import DAG
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

from airflow.providers.apache.hbase.hooks.hbase_cli import HBaseCLIHook

logger = logging.getLogger(__name__)

HBASE_CONN_ID = "hbase_thrift2"


def describe_backup_set(**context):
    """Describe backup set using execute_command."""

    params = context["params"]
    backup_set_name = params.get("backup_set_name", "test_cli_set")

    hook = HBaseCLIHook(hbase_conn_id=HBASE_CONN_ID)

    command = f"backup set describe {backup_set_name}"

    result = hook.execute_command(command)
    logger.info("Backup set description:\n%s", result)
    return result


def get_backup_history(**context):
    """Get backup history for set using execute_command."""

    params = context["params"]
    backup_set_name = params.get("backup_set_name", "test_cli_set")

    hook = HBaseCLIHook(hbase_conn_id=HBASE_CONN_ID)

    command = f"backup history -s {backup_set_name}"

    result = hook.execute_command(command)
    logger.info("Backup history:\n%s", result)
    return result


def delete_backup_set(**context):
    """Delete backup set using execute_command."""

    params = context["params"]
    backup_set_name = params.get("backup_set_name", "test_cli_set")

    hook = HBaseCLIHook(hbase_conn_id=HBASE_CONN_ID)

    command = f"backup set delete {backup_set_name}"

    result = hook.execute_command(command)
    logger.info("Delete backup set result:\n%s", result)
    return result


def verify_deletion(**context):
    """Verify backup set was deleted by listing all sets."""

    params = context["params"]
    backup_set_name = params.get("backup_set_name", "test_cli_set")

    hook = HBaseCLIHook(hbase_conn_id=HBASE_CONN_ID)

    command = "backup set list"

    result = hook.execute_command(command)
    logger.info("Backup sets after deletion:\n%s", result)

    if backup_set_name in result:
        raise ValueError(f"Backup set {backup_set_name} still exists after deletion!")
    logger.info("Backup set %s successfully deleted", backup_set_name)

    return result


with DAG(
    dag_id="example_hbase_cli_commands",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "cli", "backup_set"],
    params={
        "backup_set_name": "test_cli_set",
        "tables": ["test_table1", "test_table2"],
    },
    doc_md=__doc__,
) as dag:
    describe_set = PythonOperator(
        task_id="describe_backup_set",
        python_callable=describe_backup_set,
    )

    get_history = PythonOperator(
        task_id="get_backup_history",
        python_callable=get_backup_history,
    )

    delete_set = PythonOperator(
        task_id="delete_backup_set",
        python_callable=delete_backup_set,
    )

    verify = PythonOperator(
        task_id="verify_deletion",
        python_callable=verify_deletion,
    )

    describe_set >> get_history >> delete_set >> verify
