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
Example Airflow DAG showing usage of Teradata Airflow Provider hook interacting with Teradata SQL DB.

This DAG assumes Airflow Connection with connection id `teradata_default` already exists in locally.
It shows how to establish connection with Teradata SQL Database server and how to run queries on it.
"""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta

import teradatasql

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook

# Constants used for below example DAG
TEST_SQL = "SELECT DATE;"
CONN_ID = "teradata_default"

# [START howto_hook_teradata]


def teradata_hook_usage():
    # This creates connection with Teradata database using default connection id 'teradata_default'
    tdh = TeradataHook()

    # This creates connection with Teradata database overriding database name from what is specified
    # in teradata_default connection id
    tdh = TeradataHook(teradata_conn_id=CONN_ID, database="database_name")

    # Verify connection to database server works
    tdh.test_connection()

    # Check connection config details
    tdh.get_uri()
    # print(tdh.get_uri())

    # This method gets back a TeradataConnection object which is created using teradatasql client internally
    conn = tdh.get_conn()

    # This method returns sqlalchemy engine connection object that points to Teradata database
    tdh.get_sqlalchemy_engine()

    # Execute select queries directly using TeradataHook
    tdh.get_records(sql=TEST_SQL)
    tdh.get_first(sql=TEST_SQL)

    # Execute any other modification queries using TeradataHook
    tdh.run(sql=TEST_SQL)
    tdh.run(sql=["SELECT 1;", "SELECT 2;"])

    # Execute insert queries
    # rows = [('User5',), ('User6',)]
    # target_fields = ["user_name"]
    # res = tdh.insert_rows(table="my_users", rows=rows, target_fields=target_fields)

    # Get cursor object from connection to manually run queries and get results
    # Read more about using teradatasql connection here: https://pypi.org/project/teradatasql/
    cursor = tdh.get_cursor()
    cursor.execute(TEST_SQL).fetchall()
    [d[0] for d in cursor.description]
    try:
        # print("Test executing queries in Teradata database...")
        with closing(tdh.get_conn()) as conn, closing(conn.cursor()) as cursor:
            cursor.execute("SELECT DATE")
            conn.commit()
            print("Ran the query on Teradata database")
    except (teradatasql.OperationalError, teradatasql.Warning):
        # print("Error running query on Teradata database")
        raise

    # Get query result as pandas dataframe
    tdh.get_pandas_df(sql=TEST_SQL)

    # Get query results as chunks of rows as pandas generator
    gen = tdh.get_pandas_df_by_chunks(sql=TEST_SQL, chunksize=2)
    while True:
        try:
            next(gen)
        except StopIteration:
            break
        # print(rows)

    # Saving data to a staging table using pandas to_sql
    # conn = tdh.get_sqlalchemy_engine()
    # df.to_sql("temp_my_users", con=conn, if_exists="replace")


# [END howto_hook_teradata]


with DAG(
    dag_id="example_teradata_hook",
    description="""Sample usage of the TeradataHook airflow provider module""",
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    tags=["example"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=6),
    },
) as dag:
    show_teradata_hook_usage = PythonOperator(
        task_id="show_teradata_hook_usage", python_callable=teradata_hook_usage
    )
