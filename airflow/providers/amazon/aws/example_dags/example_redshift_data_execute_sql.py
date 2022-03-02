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

from datetime import datetime, timedelta
from os import getenv

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

# [START howto_operator_redshift_data_env_variables]
REDSHIFT_CLUSTER_IDENTIFIER = getenv("REDSHIFT_CLUSTER_IDENTIFIER", "test-cluster")
REDSHIFT_DATABASE = getenv("REDSHIFT_DATABASE", "test-database")
REDSHIFT_DATABASE_USER = getenv("REDSHIFT_DATABASE_USER", "awsuser")
# [END howto_operator_redshift_data_env_variables]

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


# [START howto_redshift_data]
@dag(
    dag_id='example_redshift_data',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    catchup=False,
)
def example_redshift_data():
    @task(task_id="output_results")
    def output_results_fn(id):
        """This is a python decorator task that returns a Redshift query"""
        hook = RedshiftDataHook()

        resp = hook.get_statement_result(
            id=id,
        )
        print(resp)
        return resp

    # Run a SQL statement and wait for completion
    redshift_query = RedshiftDataOperator(
        task_id='redshift_query',
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_DATABASE_USER,
        sql=REDSHIFT_QUERY,
        poll_interval=POLL_INTERVAL,
        await_result=True,
    )

    # Using a task-decorated function to output the list of tables in a Redshift cluster
    output_results_fn(redshift_query.output)


example_redshift_data_dag = example_redshift_data()
# [END howto_redshift_data]
