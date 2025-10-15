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
"""
Example Airflow DAG to show usage of DdlOperator.

This DAG assumes an Airflow Connection with connection id `teradata_default` already exists locally.
It demonstrates how to use DdlOperator to create, drop, alter, and rename Teradata tables and indexes.
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.tpt import DdlOperator
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START ddl_operator_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_tpt"
CONN_ID = "teradata_default"
SSH_CONN_ID = "ssh_default"

# Define file paths and table names for the test
SYSTEM_TESTS_DIR = os.path.abspath(os.path.dirname(__file__))

params = {
    "SOURCE_TABLE": "source_table",
    "TARGET_TABLE": "target_table",
}


with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": CONN_ID, "params": params},
) as dag:
    # [START ddl_operator_howto_guide_drop_table]
    # Drop tables if they exist
    drop_table = DdlOperator(
        task_id="drop_table",
        ddl=[
            "DROP TABLE {{ params.SOURCE_TABLE }};",
            "DROP TABLE {{ params.SOURCE_TABLE }}_UV;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_ET;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_WT;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_Log;",
            "DROP TABLE {{ params.TARGET_TABLE }};",
            "DROP TABLE {{ params.TARGET_TABLE }}_UV;",
            "DROP TABLE {{ params.TARGET_TABLE }}_ET;",
            "DROP TABLE {{ params.TARGET_TABLE }}_WT;",
            "DROP TABLE {{ params.TARGET_TABLE }}_Log;",
        ],
        error_list=[3706, 3803, 3807],
    )
    # [END ddl_operator_howto_guide_drop_table]

    # [START ddl_operator_howto_guide_create_table]
    create_source_table = DdlOperator(
        task_id="create_source_table",
        ddl=[
            "CREATE TABLE {{ params.SOURCE_TABLE }} ( \
                first_name VARCHAR(100), \
                last_name VARCHAR(100), \
                employee_id VARCHAR(10), \
                department VARCHAR(50) \
            );"
        ],
    )

    create_target_table = DdlOperator(
        task_id="create_target_table",
        ddl=[
            "CREATE TABLE {{ params.TARGET_TABLE }} ( \
                first_name VARCHAR(100), \
                last_name VARCHAR(100), \
                employee_id VARCHAR(10), \
                department VARCHAR(50) \
            );"
        ],
    )
    # [END ddl_operator_howto_guide_create_table]

    # [START ddl_operator_howto_guide_create_index]
    create_index_on_source = DdlOperator(
        task_id="create_index_on_source",
        ddl=["CREATE INDEX idx_employee_id (employee_id) ON {{ params.SOURCE_TABLE }};"],
    )
    # [END ddl_operator_howto_guide_create_index]

    # [START ddl_operator_howto_guide_rename_table]
    rename_target_table = DdlOperator(
        task_id="rename_target_table",
        ddl=[
            "RENAME TABLE {{ params.TARGET_TABLE }} TO {{ params.TARGET_TABLE }}_renamed;",
            "DROP TABLE {{ params.TARGET_TABLE }}_renamed",
        ],
    )
    # [END ddl_operator_howto_guide_rename_table]

    # [START ddl_operator_howto_guide_drop_index]
    drop_index_on_source = DdlOperator(
        task_id="drop_index_on_source",
        ddl=["DROP INDEX idx_employee_id ON {{ params.SOURCE_TABLE }};"],
        error_list=[3706, 3803, 3807],
    )
    # [END ddl_operator_howto_guide_drop_index]

    # [START ddl_operator_howto_guide_alter_table]
    alter_source_table = DdlOperator(
        task_id="alter_source_table",
        ddl=["ALTER TABLE {{ params.SOURCE_TABLE }} ADD hire_date DATE;"],
    )
    # [END ddl_operator_howto_guide_alter_table]

    # Define the task dependencies
    (
        drop_table
        >> create_source_table
        >> create_target_table
        >> create_index_on_source
        >> rename_target_table
        >> drop_index_on_source
        >> alter_source_table
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
# [END ddl_operator_howto_guide]
