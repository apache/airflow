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
"""
Example Airflow DAG to show usage of teradata to teradata transfer operator.

The transfer operator connects to source teradata server, runs query to fetch data from source
and inserts that data into destination teradata database server. It assumes tables already exists.
The example DAG below assumes Airflow Connection with connection id `teradata_default` already exists.
It creates sample my_users table at source and destination, sets up sample data at source and then
runs transfer operator to copy data to corresponding table on destination server.
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
    from airflow.providers.teradata.transfers.teradata_to_teradata import (
        TeradataToTeradataOperator,
    )
except ImportError:
    pytest.skip(
        "Teradata provider apache-airflow-provider-teradata not available",
        allow_module_level=True,
    )

# [START teradata_to_teradata_transfer_operator_howto_guide]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata_to_teradata_transfer_operator"
CONN_ID = "teradata_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": CONN_ID},
) as dag:
    # [START teradata_to_teradata_transfer_operator_howto_guide_create_src_table]
    create_src_table = TeradataOperator(
        task_id="create_src_table",
        sql="""
            CREATE TABLE my_users_src,
            FALLBACK (
                user_id decimal(10,0) NOT NULL GENERATED ALWAYS AS IDENTITY (
                    START WITH 1
                    INCREMENT BY 1
                    MINVALUE 1
                    MAXVALUE 2147483647
                    NO CYCLE),
                user_name VARCHAR(30),
                gender CHAR(1) DEFAULT 'M',
                birth_date DATE FORMAT 'YYYY-MM-DD' NOT NULL DEFAULT DATE '2023-01-01'
            ) PRIMARY INDEX (user_id);
        """,
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide_create_src_table]
    # [START teradata_to_teradata_transfer_operator_howto_guide_create_dest_table]
    create_dest_table = TeradataOperator(
        task_id="create_dest_table",
        sql="""
            CREATE TABLE my_users_dest,
            FALLBACK (
                user_id decimal(10,0) NOT NULL GENERATED ALWAYS AS IDENTITY (
                    START WITH 1
                    INCREMENT BY 1
                    MINVALUE 1
                    MAXVALUE 2147483647
                    NO CYCLE),
                user_name VARCHAR(30),
                gender CHAR(1) DEFAULT 'M',
                birth_date DATE FORMAT 'YYYY-MM-DD' NOT NULL DEFAULT DATE '2023-01-01'
            ) PRIMARY INDEX (user_id);
        """,
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide_create_dest_table]
    # [START teradata_to_teradata_transfer_operator_howto_guide_insert_data_src]
    insert_data_src = TeradataOperator(
        task_id="insert_data_src",
        sql="""
            INSERT INTO my_users_src(user_name) VALUES ('User1');
            INSERT INTO my_users_src(user_name) VALUES ('User2');
            INSERT INTO my_users_src(user_name) VALUES ('User3');
        """,
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide_insert_data_src]
    # [START teradata_to_teradata_transfer_operator_howto_guide_read_data_src]
    read_data_src = TeradataOperator(
        task_id="read_data_src",
        sql="SELECT TOP 10 * from my_users_src order by user_id desc;",
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide_read_data_src]
    # [START teradata_to_teradata_transfer_operator_howto_guide_transfer_data]
    transfer_data = TeradataToTeradataOperator(
        task_id="transfer_data",
        dest_teradata_conn_id="teradata_default",
        destination_table="my_users_dest",
        source_teradata_conn_id="teradata_default",
        sql="select * from my_users_src",
        sql_params={},
        rows_chunk=2,
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide_transfer_data]
    # [START teradata_to_teradata_transfer_operator_howto_guide_read_data_dest]
    read_data_dest = TeradataOperator(
        task_id="read_data_dest",
        sql="SELECT TOP 10 * from my_users_dest order by user_id desc;",
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide_read_data_dest]
    # [START teradata_to_teradata_transfer_operator_howto_guide_drop_src_table]
    drop_src_table = TeradataOperator(
        task_id="drop_src_table",
        sql=" DROP TABLE my_users_src;",
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide_drop_src_table]
    # [START teradata_to_teradata_transfer_operator_howto_guide_drop_dest_table]
    drop_dest_table = TeradataOperator(
        task_id="drop_dest_table",
        sql="DROP TABLE my_users_dest;",
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide_drop_dest_table]
    (
        create_src_table
        >> create_dest_table
        >> insert_data_src
        >> read_data_src
        >> transfer_data
        >> read_data_dest
        >> drop_src_table
        >> drop_dest_table
    )
    # [END teradata_to_teradata_transfer_operator_howto_guide]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
