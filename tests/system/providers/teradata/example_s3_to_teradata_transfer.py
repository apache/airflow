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
Example Airflow DAG to show usage of teradata to teradata transfer operator

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

from airflow.providers.teradata.transfers.s3_to_teradata import S3ToTeradataOperator

from airflow import DAG

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
    from airflow.providers.teradata.transfers.teradata_to_teradata import TeradataToTeradataOperator
except ImportError:
    pytest.skip("Teradata provider apache-airflow-provider-teradata not available", allow_module_level=True)

# [START s3_to_teradata_transfer_operator_howto_guide]

# transfer_data_csv, read_data_table_csv, drop_table_csv, transfer_data_json, read_data_table_json,
# drop_table_json, transfer_data_parquet, read_data_table_parquet, drop_table_parquet
# transfer_data_access, read_data_table_access, drop_table_access are examples of tasks created
# by instantiating the S3ToTeradata Transfer Operator.

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_s3_to_teradata_transfer_operator"
CONN_ID = "teradata_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"conn_id": "teradata_default"},
) as dag:
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_csv_data_s3_to_teradata]
    transfer_data_csv = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_csv",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/CSVDATA/",
        teradata_table="example_s3_teradata_csv",
        aws_conn_id="aws_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_csv_data_s3_to_teradata]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    read_data_table_csv = TeradataOperator(
        task_id="read_data_table_csv",
        conn_id=CONN_ID,
        sql="""
                    SELECT * from example_s3_teradata_csv;
                """,
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    drop_table_csv = TeradataOperator(
        task_id="drop_table_csv",
        conn_id=CONN_ID,
        sql="""
                DROP TABLE example_s3_teradata_csv;
            """,
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_json_data_s3_to_teradata]
    transfer_data_json = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_json",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/JSONDATA/",
        teradata_table="example_s3_teradata_json",
        aws_conn_id="aws_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_json_data_s3_to_teradata]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_json]
    read_data_table_json = TeradataOperator(
        task_id="read_data_table_json",
        conn_id=CONN_ID,
        sql="""
                    SELECT * from example_s3_teradata_json;
                """,
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_json]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table_json]
    drop_table_json = TeradataOperator(
        task_id="drop_table_json",
        conn_id=CONN_ID,
        sql="""
                    DROP TABLE example_s3_teradata_json;
                """,
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table_json]
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_parquet_data_s3_to_teradata]
    transfer_data_parquet = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_parquet",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/PARQUETDATA/",
        teradata_table="example_s3_teradata_parquet",
        aws_conn_id="aws_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_parquet_data_s3_to_teradata]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_parquet]
    read_data_table_parquet = TeradataOperator(
        task_id="read_data_table_parquet",
        conn_id=CONN_ID,
        sql="""
                    SELECT * from example_s3_teradata_parquet;
                """,
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_parquet]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table_parquet]
    drop_table_parquet = TeradataOperator(
        task_id="drop_table_parquet",
        conn_id=CONN_ID,
        sql="""
                    DROP TABLE example_s3_teradata_parquet;
                """,
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table_parquet]
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_access_teradata]
    transfer_data_access = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_access",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/JSONDATA/",
        teradata_table="example_s3_teradata_access",
        aws_access_key="",
        aws_access_secret="",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_access_teradata]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_access]
    read_data_table_access = TeradataOperator(
        task_id="read_data_table_access",
        conn_id=CONN_ID,
        sql="""
                SELECT * from example_s3_teradata_access;
                """,
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_access]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table_access]
    drop_table_access = TeradataOperator(
        task_id="drop_table_access",
        conn_id=CONN_ID,
        sql="""
                   DROP TABLE example_s3_teradata_access;
               """,
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table_access]
    (
        transfer_data_csv,
        transfer_data_json,
        transfer_data_parquet,
        transfer_data_access,
        read_data_table_csv,
        read_data_table_json,
        read_data_table_parquet,
        read_data_table_access,
        drop_table_csv,
        drop_table_json,
        drop_table_parquet,
        drop_table_access
    )
    # [END s3_to_teradata_transfer_operator_howto_guide]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
