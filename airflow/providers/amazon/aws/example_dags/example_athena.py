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
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator, S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor

S3_BUCKET = getenv("S3_BUCKET", "test-bucket")
S3_KEY = getenv('S3_KEY', 'athena-demo')
ATHENA_TABLE = getenv('ATHENA_TABLE', 'test_table')
ATHENA_DATABASE = getenv('ATHENA_DATABASE', 'default')

SAMPLE_DATA = """"Alice",20
"Bob",25
"Charlie",30
"""
SAMPLE_FILENAME = 'airflow_sample.csv'


@task
def read_results_from_s3(query_execution_id):
    s3_hook = S3Hook()
    file_obj = s3_hook.get_conn().get_object(Bucket=S3_BUCKET, Key=f'{S3_KEY}/{query_execution_id}.csv')
    file_content = file_obj['Body'].read().decode('utf-8')
    print(file_content)


QUERY_CREATE_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE} ( `name` string, `age` int )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ( 'serialization.format' = ',', 'field.delim' = ','
) LOCATION 's3://{S3_BUCKET}/{S3_KEY}/{ATHENA_TABLE}'
TBLPROPERTIES ('has_encrypted_data'='false')
"""

QUERY_READ_TABLE = f"""
SELECT * from {ATHENA_DATABASE}.{ATHENA_TABLE}
"""

QUERY_DROP_TABLE = f"""
DROP TABLE IF EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE}
"""

with DAG(
    dag_id='example_athena',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    upload_sample_data = S3CreateObjectOperator(
        task_id='upload_sample_data',
        s3_bucket=S3_BUCKET,
        s3_key=f'{S3_KEY}/{ATHENA_TABLE}/{SAMPLE_FILENAME}',
        data=SAMPLE_DATA,
        replace=True,
    )

    create_table = AthenaOperator(
        task_id='create_table',
        query=QUERY_CREATE_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
    )

    # [START howto_athena_operator]
    read_table = AthenaOperator(
        task_id='read_table',
        query=QUERY_READ_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
    )
    # [END howto_athena_operator]

    # [START howto_athena_sensor]
    await_query = AthenaSensor(
        task_id='await_query',
        query_execution_id=read_table.output,
    )
    # [END howto_athena_sensor]

    drop_table = AthenaOperator(
        task_id='drop_table',
        query=QUERY_DROP_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
    )

    remove_s3_files = S3DeleteObjectsOperator(
        task_id='remove_s3_files',
        bucket=S3_BUCKET,
        prefix=S3_KEY,
    )

    (
        upload_sample_data
        >> create_table
        >> read_table
        >> await_query
        >> read_results_from_s3(read_table.output)
        >> drop_table
        >> remove_s3_files
    )
