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
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator

with DAG(
    dag_id="transfer_s3_to_sql",
    start_date=datetime(2023, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:

    # [START howto_transfer_s3_to_sql]
    #
    # Use column_list = List[str] if you want to pass the column header explicitly
    # Be sure that the data of the CSV file corresponents to the column_list passed
    #
    transfer_s3_to_sql_explicit_column_names = S3ToSqlOperator(
        task_id="transfer_s3_to_sql",
        aws_conn_id="aws_default",
        s3_bucket="bucket",
        s3_key="data_without_header.csv",
        table="data_table",
        column_list=["Column1", "Column2"],
        schema="schema",
        commit_every=5000,
    )
    # [END howto_transfer_s3_to_sql]

    # [START howto_transfer_s3_to_sql_inferred_column_names]
    # Use column_list = 'infer' if the column names should be read from the first
    # line of the CSV file
    #
    transfer_s3_to_sql_inferred_column_names = S3ToSqlOperator(
        task_id="transfer_s3_to_sql",
        aws_conn_id="aws_default",
        s3_bucket="bucket",
        s3_key="data_with_headers.csv",
        table="data_table",
        column_list="infer",
        schema="schema",
        commit_every=5000,
    )

    # [END howto_transfer_s3_to_sql_inferred_column_names]

    # [START howto_transfer_s3_to_sql_csv_reader_kwargs]
    # If you want to use `;` as the delimiter instead of default `,`,
    # pass it in `csv_reader_kwargs`
    transfer_s3_to_sql_inferred_column_names = S3ToSqlOperator(
        task_id="transfer_s3_to_sql",
        aws_conn_id="aws_default",
        s3_bucket="bucket",
        s3_key="data_with_headers.csv",
        table="data_table",
        column_list="infer",
        csv_reader_kwargs={"delimiter": ";"},
        schema="schema",
        commit_every=5000,
    )

    # [END howto_transfer_s3_to_sql_csv_reader_kwargs]
