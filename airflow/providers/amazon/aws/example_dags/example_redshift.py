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
This is an example dag for using `RedshiftOperator` to authenticate with Amazon Redshift
using IAM authentication then executing a simple select statement
"""
# [START redshift_operator_howto_guide]
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="example_redshift", start_date=days_ago(1), schedule_interval=None, tags=['example']) as dag:
    # [START howto_operator_s3_to_redshift_create_table]
    setup__task_create_table = RedshiftOperator(
        task_id='setup__create_table',
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
    )
    # [END howto_operator_s3_to_redshift_create_table]
    # [START howto_operator_s3_to_redshift_populate_table]
    task_insert_data = RedshiftOperator(
        task_id='task_insert_data',
        sql=[
            "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
        ],
    )
    # [END howto_operator_s3_to_redshift_populate_table]
    # [START howto_operator_s3_to_redshift_get_all_rows]
    task_get_all_table_data = RedshiftOperator(task_id='task_get_all_table_data', sql="SELECT * FROM fruit;")
    # [END howto_operator_s3_to_redshift_get_all_rows]
    # [START howto_operator_s3_to_redshift_get_with_filter]
    task_get_with_filter = RedshiftOperator(
        task_id='task_get_with_filter',
        sql="SELECT * FROM fruit WHERE color = '{{ params.color }}';",
        params={'color': 'Red'},
    )
    # [END howto_operator_s3_to_redshift_get_with_filter]

    setup__task_create_table >> task_insert_data >> task_get_all_table_data >> task_get_with_filter
# [END redshift_operator_howto_guide]
