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

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

S3_BUCKET_NAME = getenv("S3_BUCKET_NAME", "s3_bucket_name")
S3_KEY = getenv("S3_KEY", "s3_filename")
REDSHIFT_TABLE = getenv("REDSHIFT_TABLE", "redshift_table")


@task(task_id='setup__add_sample_data_to_s3')
def task_add_sample_data_to_s3():
    s3_hook = S3Hook()
    s3_hook.load_string("0,Airflow", f'{S3_KEY}/{REDSHIFT_TABLE}', S3_BUCKET_NAME, replace=True)


@task(task_id='teardown__remove_sample_data_from_s3')
def task_remove_sample_data_from_s3():
    s3_hook = S3Hook()
    if s3_hook.check_for_key(f'{S3_KEY}/{REDSHIFT_TABLE}', S3_BUCKET_NAME):
        s3_hook.delete_objects(S3_BUCKET_NAME, f'{S3_KEY}/{REDSHIFT_TABLE}')


with DAG(
    dag_id="example_s3_to_redshift",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    add_sample_data_to_s3 = task_add_sample_data_to_s3()

    setup__task_create_table = RedshiftSQLOperator(
        sql=f'CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE}(Id int, Name varchar)',
        task_id='setup__create_table',
    )
    # [START howto_transfer_s3_to_redshift]
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket=S3_BUCKET_NAME,
        s3_key=S3_KEY,
        schema='PUBLIC',
        table=REDSHIFT_TABLE,
        copy_options=['csv'],
        task_id='transfer_s3_to_redshift',
    )
    # [END howto_transfer_s3_to_redshift]
    teardown__task_drop_table = RedshiftSQLOperator(
        sql=f'DROP TABLE IF EXISTS {REDSHIFT_TABLE}',
        task_id='teardown__drop_table',
    )

    remove_sample_data_from_s3 = task_remove_sample_data_from_s3()

    chain(
        [add_sample_data_to_s3, setup__task_create_table],
        task_transfer_s3_to_redshift,
        [teardown__task_drop_table, remove_sample_data_from_s3],
    )
