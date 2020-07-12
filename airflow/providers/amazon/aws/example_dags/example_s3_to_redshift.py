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
This is an example dag for using `S3ToRedshiftOperator` to copy a S3 key into a Redshift table.
"""

from os import getenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# [START howto_operator_s3_to_redshift_env_variables]
S3_BUCKET = getenv("S3_BUCKET", "test-bucket")
S3_KEY = getenv("S3_KEY", "key")
REDSHIFT_TABLE = getenv("REDSHIFT_TABLE", "test_table")
# [END howto_operator_s3_to_redshift_env_variables]

default_args = {"start_date": days_ago(1)}


def _add_sample_data_to_s3():
    s3_hook = S3Hook()
    s3_hook.load_string("0,Airflow", f'{S3_KEY}/{REDSHIFT_TABLE}', S3_BUCKET, replace=True)


def _remove_sample_data_from_s3():
    s3_hook = S3Hook()
    if s3_hook.check_for_key(f'{S3_KEY}/{REDSHIFT_TABLE}', S3_BUCKET):
        s3_hook.delete_objects(S3_BUCKET, f'{S3_KEY}/{REDSHIFT_TABLE}')


with DAG(
    dag_id="example_s3_to_redshift",
    default_args=default_args,
    schedule_interval=None,
    tags=['example']
) as dag:
    setup__task_add_sample_data_to_s3 = PythonOperator(
        python_callable=_add_sample_data_to_s3,
        task_id='setup__add_sample_data_to_s3'
    )
    setup__task_create_table = PostgresOperator(
        sql=f'CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE}(Id int, Name varchar)',
        postgres_conn_id='redshift_default',
        task_id='setup__create_table'
    )
    # [START howto_operator_s3_to_redshift_task_1]
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema="PUBLIC",
        table=REDSHIFT_TABLE,
        copy_options=['csv'],
        task_id='transfer_s3_to_redshift'
    )
    # [END howto_operator_s3_to_redshift_task_1]
    teardown__task_drop_table = PostgresOperator(
        sql=f'DROP TABLE IF EXISTS {REDSHIFT_TABLE}',
        postgres_conn_id='redshift_default',
        task_id='teardown__drop_table'
    )
    teardown__task_remove_sample_data_from_s3 = PythonOperator(
        python_callable=_remove_sample_data_from_s3,
        task_id='teardown__remove_sample_data_from_s3'
    )
    [
        setup__task_add_sample_data_to_s3,
        setup__task_create_table
    ] >> task_transfer_s3_to_redshift >> [
        teardown__task_drop_table,
        teardown__task_remove_sample_data_from_s3
    ]
