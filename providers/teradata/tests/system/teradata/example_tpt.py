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
Example Airflow DAG to show usage of DdlOperator and TdLoadOperator.

This DAG assumes an Airflow Connection with connection id `teradata_default` already exists locally.
It demonstrates how to use DdlOperator to create, drop, alter, and rename Teradata tables and indexes.
It also shows how to load data from a file to a Teradata table, export data from a Teradata table to a file and
transfer data between two Teradata tables (potentially across different databases).
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.tpt import DdlOperator, TdLoadOperator
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START tdload_operator_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_tpt"
CONN_ID = "teradata_default"
SSH_CONN_ID = "ssh_default"

# Define file paths and table names for the test
SYSTEM_TESTS_DIR = os.path.abspath(os.path.dirname(__file__))
SOURCE_FILE = os.path.join(SYSTEM_TESTS_DIR, "tdload_src_file.txt")
TARGET_FILE = os.path.join(SYSTEM_TESTS_DIR, "tdload_target_file.txt")

params = {
    "SOURCE_TABLE": "source_table",
    "TARGET_TABLE": "target_table",
    "SOURCE_FILE": SOURCE_FILE,
    "TARGET_FILE": TARGET_FILE,
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

    # [START tdload_operator_howto_guide_load_from_file]
    load_file = TdLoadOperator(
        task_id="load_file",
        source_file_name="{{ params.SOURCE_FILE }}",
        target_table="{{ params.SOURCE_TABLE }}",
        source_format="Delimited",
        source_text_delimiter="|",
    )
    # [END tdload_operator_howto_guide_load_from_file]

    # [START tdload_operator_howto_guide_export_data]
    export_data = TdLoadOperator(
        task_id="export_data",
        source_table="{{ params.SOURCE_TABLE }}",
        target_file_name="{{ params.TARGET_FILE }}",
        target_format="Delimited",
        target_text_delimiter=";",
    )
    # [END tdload_operator_howto_guide_export_data]

    # [START tdload_operator_howto_guide_transfer_data]
    transfer_data = TdLoadOperator(
        task_id="transfer_data",
        source_table="{{ params.SOURCE_TABLE }}",
        target_table="{{ params.TARGET_TABLE }}",
        target_teradata_conn_id=CONN_ID,
    )
    # [END tdload_operator_howto_guide_transfer_data]

    create_select_dest_table = DdlOperator(
        task_id="create_select_dest_table",
        ddl=[
            "DROP TABLE {{ params.SOURCE_TABLE }}_select_dest;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_select_log;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_select_err1;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_select_err2;",
            "CREATE TABLE {{ params.SOURCE_TABLE }}_select_dest ( \
                first_name VARCHAR(100), \
                last_name VARCHAR(100), \
                employee_id VARCHAR(10), \
                department VARCHAR(50) \
            );",
        ],
        error_list=[3706, 3803, 3807],
    )

    # [START tdload_operator_howto_guide_transfer_data_select_stmt]
    # TdLoadOperator using select statement as source
    transfer_data_select_stmt = TdLoadOperator(
        task_id="transfer_data_select_stmt",
        select_stmt="SELECT * FROM {{ params.SOURCE_TABLE }}",
        target_table="{{ params.SOURCE_TABLE }}_select_dest",
        tdload_options="--LogTable {{ params.SOURCE_TABLE }}_select_log --ErrorTable1 {{ params.SOURCE_TABLE }}_select_err1 --ErrorTable2 {{ params.SOURCE_TABLE }}_select_err2",
        target_teradata_conn_id=CONN_ID,
    )
    # [END tdload_operator_howto_guide_transfer_data_select_stmt]

    # Create table for insert statement test
    create_insert_dest_table = DdlOperator(
        task_id="create_insert_dest_table",
        ddl=[
            "DROP TABLE {{ params.SOURCE_TABLE }}_insert_dest;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_insert_log;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_insert_err1;",
            "DROP TABLE {{ params.SOURCE_TABLE }}_insert_err2;",
            "CREATE TABLE {{ params.SOURCE_TABLE }}_insert_dest ( \
                first_name VARCHAR(100), \
                last_name VARCHAR(100), \
                employee_id VARCHAR(10), \
                department VARCHAR(50) \
            );",
        ],
        error_list=[3706, 3803, 3807],
    )

    # [START tdload_operator_howto_guide_transfer_data_insert_stmt]
    transfer_data_insert_stmt = TdLoadOperator(
        task_id="transfer_data_insert_stmt",
        source_table="{{ params.SOURCE_TABLE }}",
        insert_stmt="INSERT INTO {{ params.SOURCE_TABLE }}_insert_dest VALUES (?, ?, ?, ?)",
        target_table="{{ params.SOURCE_TABLE }}_insert_dest",
        tdload_options="--LogTable {{ params.SOURCE_TABLE }}_insert_log --ErrorTable1 {{ params.SOURCE_TABLE }}_insert_err1 --ErrorTable2 {{ params.SOURCE_TABLE }}_insert_err2",
        tdload_job_name="tdload_job_insert_stmt",
        target_teradata_conn_id=CONN_ID,
    )
    # [END tdload_operator_howto_guide_transfer_data_insert_stmt]

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
        >> load_file
        >> export_data
        >> transfer_data
        >> create_select_dest_table
        >> transfer_data_select_stmt
        >> create_insert_dest_table
        >> transfer_data_insert_stmt
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
# [END tdload_operator_howto_guide]
