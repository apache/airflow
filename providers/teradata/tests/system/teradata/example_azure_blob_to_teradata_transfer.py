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
Example Airflow DAG to show usage of AzureBlobStorageToTeradataOperator

The transfer operator moves files in CSV, JSON, and PARQUET formats from Azure Blob storage
to Teradata tables. In the example Directed Acyclic Graph (DAG) below, it assumes Airflow
Connections with the IDs `teradata_default` and `wasb_default` already exist. The DAG creates
tables using data from the Azure Blob location, reports the number of rows inserted into
the table, and subsequently drops the table.
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
    from airflow.providers.teradata.transfers.azure_blob_to_teradata import AzureBlobStorageToTeradataOperator
except ImportError:
    pytest.skip("Teradata provider apache-airflow-provider-teradata not available", allow_module_level=True)

# [START azure_blob_to_teradata_transfer_operator_howto_guide]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_azure_blob_to_teradata_transfer_operator"
CONN_ID = "teradata_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": CONN_ID},
) as dag:
    # [START azure_blob__to_teradata_transfer_operator_howto_guide_transfer_data_public_blob_to_teradata_csv]
    transfer_data_csv = AzureBlobStorageToTeradataOperator(
        task_id="transfer_data_blob_to_teradata_csv",
        blob_source_key="/az/akiaxox5jikeotfww4ul.blob.core.windows.net/td-usgs/CSVDATA/09380000/2018/06/",
        public_bucket=True,
        teradata_table="example_blob_teradata_csv",
        teradata_conn_id="teradata_default",
        azure_conn_id="wasb_default",
        trigger_rule="all_done",
    )
    # [END azure_blob__to_teradata_transfer_operator_howto_guide_transfer_data_public_blob_to_teradata_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    read_data_table_csv = TeradataOperator(
        task_id="read_data_table_csv",
        sql="SELECT count(1) from example_blob_teradata_csv;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    drop_table_csv = TeradataOperator(
        task_id="drop_table_csv",
        sql="DROP TABLE example_blob_teradata_csv;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_access_blob_to_teradata_csv]
    transfer_key_data_csv = AzureBlobStorageToTeradataOperator(
        task_id="transfer_key_data_blob_to_teradata_csv",
        blob_source_key="/az/airflowteradata.blob.core.windows.net/csvdata/",
        teradata_table="example_blob_teradata_csv",
        azure_conn_id="wasb_default",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_access_blob_to_teradata_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    read_key_data_table_csv = TeradataOperator(
        task_id="read_key_data_table_csv",
        conn_id=CONN_ID,
        sql="SELECT count(1) from example_blob_teradata_csv;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    drop_key_table_csv = TeradataOperator(
        task_id="drop_key_table_csv",
        conn_id=CONN_ID,
        sql="DROP TABLE example_blob_teradata_csv;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_create_authorization]
    create_azure_authorization = TeradataOperator(
        task_id="create_azure_authorization",
        conn_id=CONN_ID,
        sql="CREATE AUTHORIZATION azure_authorization USER '{{ var.value.get('AZURE_BLOB_ACCOUNTNAME') }}' PASSWORD '{{ var.value.get('AZURE_BLOB_ACCOUNT_SECRET_KEY') }}' ",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_create_authorization]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_authorization_blob_to_teradata_csv]
    transfer_auth_data_csv = AzureBlobStorageToTeradataOperator(
        task_id="transfer_auth_data_blob_to_teradata_csv",
        blob_source_key="/az/airflowteradata.blob.core.windows.net/csvdata/",
        teradata_table="example_blob_teradata_csv",
        teradata_authorization_name="azure_authorization",
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_authorization_blob_to_teradata_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    read_auth_data_table_csv = TeradataOperator(
        task_id="read_auth_data_table_csv",
        conn_id=CONN_ID,
        sql="SELECT count(1) from example_blob_teradata_csv;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    drop_auth_table_csv = TeradataOperator(
        task_id="drop_auth_table_csv",
        conn_id=CONN_ID,
        sql="DROP TABLE example_blob_teradata_csv;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_csv]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_drop_authorization]
    drop_auth = TeradataOperator(
        task_id="drop_auth",
        conn_id=CONN_ID,
        sql="DROP AUTHORIZATION azure_authorization;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_drop_authorization]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_json]
    transfer_data_json = AzureBlobStorageToTeradataOperator(
        task_id="transfer_data_blob_to_teradata_json",
        blob_source_key="/az/akiaxox5jikeotfww4ul.blob.core.windows.net/td-usgs/JSONDATA/09380000/2018/06/",
        teradata_table="example_blob_teradata_json",
        public_bucket=True,
        teradata_conn_id="teradata_default",
        azure_conn_id="wasb_default",
        trigger_rule="all_done",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_json]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_json]
    read_data_table_json = TeradataOperator(
        task_id="read_data_table_json",
        sql="SELECT count(1) from example_blob_teradata_json;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_json]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_json]
    drop_table_json = TeradataOperator(
        task_id="drop_table_json",
        sql="DROP TABLE example_blob_teradata_json;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_json]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_parquet]
    transfer_data_parquet = AzureBlobStorageToTeradataOperator(
        task_id="transfer_data_blob_to_teradata_parquet",
        blob_source_key="/az/akiaxox5jikeotfww4ul.blob.core.windows.net/td-usgs/PARQUETDATA/09394500/2018/06/",
        teradata_table="example_blob_teradata_parquet",
        public_bucket=True,
        teradata_conn_id="teradata_default",
        trigger_rule="all_done",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_transfer_data_blob_to_teradata_parquet]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_parquet]
    read_data_table_parquet = TeradataOperator(
        task_id="read_data_table_parquet",
        sql="SELECT count(1) from example_blob_teradata_parquet;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_read_data_table_parquet]
    # [START azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_parquet]
    drop_table_parquet = TeradataOperator(
        task_id="drop_table_parquet",
        sql="DROP TABLE example_blob_teradata_parquet;",
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide_drop_table_parquet]

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
        >> create_azure_authorization
        >> transfer_auth_data_csv
        >> read_auth_data_table_csv
        >> drop_auth_table_csv
        >> drop_auth
    )
    # [END azure_blob_to_teradata_transfer_operator_howto_guide]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
