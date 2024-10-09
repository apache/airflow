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
Example Airflow DAG to show usage of S3StorageToTeradataOperator.

The transfer operator moves files in CSV, JSON, and PARQUET formats from S3
to Teradata tables. In the example Directed Acyclic Graph (DAG) below, it assumes Airflow
Connections with the IDs `teradata_default` and `aws_default` already exist. The DAG creates
tables using data from the S3, reports the number of rows inserted into
the table, and subsequently drops the table.
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG
from airflow.providers.teradata.transfers.s3_to_teradata import S3ToTeradataOperator

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
except ImportError:
    pytest.skip("Teradata provider apache-airflow-provider-teradata not available", allow_module_level=True)

# [START s3_to_teradata_transfer_operator_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_s3_to_teradata_transfer_operator"
CONN_ID = "teradata_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": CONN_ID},
) as dag:
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_public_s3_to_teradata_csv]
    transfer_data_csv = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_csv",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/CSVDATA/09394500/2018/06/",
        public_bucket=True,
        teradata_table="example_s3_teradata_csv",
        aws_conn_id="aws_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_public_s3_to_teradata_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    read_data_table_csv = TeradataOperator(
        task_id="read_data_table_csv",
        conn_id=CONN_ID,
        sql="SELECT * from example_s3_teradata_csv;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    drop_table_csv = TeradataOperator(
        task_id="drop_table_csv",
        conn_id=CONN_ID,
        sql="DROP TABLE example_s3_teradata_csv;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_access_s3_to_teradata_csv]
    transfer_key_data_csv = S3ToTeradataOperator(
        task_id="transfer_key_data_s3_to_teradata_key_csv",
        s3_source_key="/s3/airflowteradatatest.s3.ap-southeast-2.amazonaws.com/",
        teradata_table="example_s3_teradata_csv",
        aws_conn_id="aws_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_access_s3_to_teradata_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    read_key_data_table_csv = TeradataOperator(
        task_id="read_key_data_table_csv",
        conn_id=CONN_ID,
        sql="SELECT * from example_s3_teradata_csv;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    drop_key_table_csv = TeradataOperator(
        task_id="drop_key_table_csv",
        conn_id=CONN_ID,
        sql="DROP TABLE example_s3_teradata_csv;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_create_authorization]
    create_aws_authorization = TeradataOperator(
        task_id="create_aws_authorization",
        conn_id=CONN_ID,
        sql="CREATE AUTHORIZATION aws_authorization USER '{{ var.value.get('AWS_ACCESS_KEY_ID') }}' PASSWORD '{{ var.value.get('AWS_SECRET_ACCESS_KEY') }}' ",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_create_authorization]
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_authorization_s3_to_teradata_csv]
    transfer_auth_data_csv = S3ToTeradataOperator(
        task_id="transfer_auth_data_s3_to_teradata_auth_csv",
        s3_source_key="/s3/teradata-download.s3.us-east-1.amazonaws.com/DevTools/csv/",
        teradata_table="example_s3_teradata_csv",
        teradata_authorization_name="aws_authorization",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_authorization_s3_to_teradata_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    read_auth_data_table_csv = TeradataOperator(
        task_id="read_auth_data_table_csv",
        conn_id=CONN_ID,
        sql="SELECT * from example_s3_teradata_csv;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    drop_auth_table_csv = TeradataOperator(
        task_id="drop_auth_table_csv",
        conn_id=CONN_ID,
        sql="DROP TABLE example_s3_teradata_csv;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_authorization]
    drop_auth = TeradataOperator(
        task_id="drop_auth",
        conn_id=CONN_ID,
        sql="DROP AUTHORIZATION aws_authorization;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_authorization]
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_json]
    transfer_data_json = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_json",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/JSONDATA/09394500/2018/06/",
        public_bucket=True,
        teradata_table="example_s3_teradata_json",
        aws_conn_id="aws_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_json]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_json]
    read_data_table_json = TeradataOperator(
        task_id="read_data_table_json",
        sql="SELECT * from example_s3_teradata_json;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_json]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table_json]
    drop_table_json = TeradataOperator(
        task_id="drop_table_json",
        sql="DROP TABLE example_s3_teradata_json;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table_json]
    # [START s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_parquet]
    transfer_data_parquet = S3ToTeradataOperator(
        task_id="transfer_data_s3_to_teradata_parquet",
        s3_source_key="/s3/td-usgs-public.s3.amazonaws.com/PARQUETDATA/09394500/2018/06/",
        public_bucket=True,
        teradata_table="example_s3_teradata_parquet",
        aws_conn_id="aws_default",
        trigger_rule="all_done",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_transfer_data_s3_to_teradata_parquet]
    # [START s3_to_teradata_transfer_operator_howto_guide_read_data_table_parquet]
    read_data_table_parquet = TeradataOperator(
        task_id="read_data_table_parquet",
        sql="SELECT * from example_s3_teradata_parquet;",
    )
    # [END s3_to_teradata_transfer_operator_howto_guide_read_data_table_parquet]
    # [START s3_to_teradata_transfer_operator_howto_guide_drop_table]
    drop_table_parquet = TeradataOperator(
        task_id="drop_table_parquet",
        sql="DROP TABLE example_s3_teradata_parquet;",
    )

    # [END s3_to_teradata_transfer_operator_howto_guide_drop_table]
    (
        transfer_data_csv
        >> transfer_data_json
        >> transfer_data_parquet
        >> read_data_table_csv
        >> read_data_table_json
        >> read_data_table_parquet
        >> drop_table_csv
        >> drop_table_json
        >> drop_table_parquet
        >> transfer_key_data_csv
        >> read_key_data_table_csv
        >> drop_key_table_csv
        >> create_aws_authorization
        >> transfer_auth_data_csv
        >> read_auth_data_table_csv
        >> drop_auth_table_csv
        >> drop_auth
    )
    # [END s3_to_teradata_transfer_operator_howto_guide]

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
