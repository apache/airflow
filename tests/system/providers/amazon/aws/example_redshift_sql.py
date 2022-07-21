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

from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import set_env_id

ENV_ID = set_env_id()
DAG_ID = 'example_redshift_sql'

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:
    setup__task_create_table = RedshiftSQLOperator(
        task_id='setup__create_table',
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
    )

    setup__task_insert_data = RedshiftSQLOperator(
        task_id='setup__task_insert_data',
        sql=[
            "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
        ],
    )

    # [START howto_operator_redshift_sql]
    task_select_data = RedshiftSQLOperator(
        task_id='task_get_all_table_data', sql="""CREATE TABLE more_fruit AS SELECT * FROM fruit;"""
    )
    # [END howto_operator_redshift_sql]

    # [START howto_operator_redshift_sql_with_params]
    task_select_filtered_data = RedshiftSQLOperator(
        task_id='task_get_filtered_table_data',
        sql="""CREATE TABLE filtered_fruit AS SELECT * FROM fruit WHERE color = '{{ params.color }}';""",
        params={'color': 'Red'},
    )
    # [END howto_operator_redshift_sql_with_params]

    teardown__task_drop_table = RedshiftSQLOperator(
        task_id='teardown__drop_table',
        sql='DROP TABLE IF EXISTS fruit',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        setup__task_create_table,
        setup__task_insert_data,
        [task_select_data, task_select_filtered_data],
        teardown__task_drop_table,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
