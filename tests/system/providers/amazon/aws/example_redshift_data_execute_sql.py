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
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from tests.system.providers.amazon.aws.utils import fetch_variable, set_env_id

ENV_ID = set_env_id()
DAG_ID = 'example_redshift_data_execute_sql'
REDSHIFT_CLUSTER_IDENTIFIER = fetch_variable("REDSHIFT_CLUSTER_IDENTIFIER", "redshift_cluster_identifier")
REDSHIFT_DATABASE = fetch_variable("REDSHIFT_DATABASE", "redshift_database")
REDSHIFT_DATABASE_USER = fetch_variable("REDSHIFT_DATABASE_USER", "awsuser")

REDSHIFT_QUERY = """
SELECT table_schema,
       table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
      AND table_type = 'BASE TABLE'
ORDER BY table_schema,
         table_name;
            """
POLL_INTERVAL = 10


@task(task_id="output_results")
def output_query_results(statement_id):
    hook = RedshiftDataHook()
    resp = hook.conn.get_statement_result(
        Id=statement_id,
    )

    print(resp)
    return resp


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_redshift_data]
    task_query = RedshiftDataOperator(
        task_id='redshift_query',
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_DATABASE_USER,
        sql=REDSHIFT_QUERY,
        poll_interval=POLL_INTERVAL,
        await_result=True,
    )
    # [END howto_operator_redshift_data]

    task_output = output_query_results(task_query.output)

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
