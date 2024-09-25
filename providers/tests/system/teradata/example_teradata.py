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
Example Airflow DAG to show usage of TeradataOperator.

This DAG assumes Airflow Connection with connection id `teradata_default` already exists in locally. It
shows how to use create, update, delete and select teradata statements with TeradataOperator as tasks in
airflow dags using TeradataStoredProcedureOperator.
"""

from __future__ import annotations

import datetime
import os

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

# [START teradata_operator_howto_guide]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata"
CONN_ID = "teradata_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": CONN_ID},
) as dag:
    # [START teradata_operator_howto_guide_create_table]
    create_table = TeradataOperator(
        task_id="create_table",
        sql=r"""
        CREATE TABLE Country (
            country_id INTEGER,
            name CHAR(25),
            continent CHAR(25)
        );
        """,
    )
    # [END teradata_operator_howto_guide_create_table]

    # [START teradata_operator_howto_guide_create_table_from_external_file]
    create_table_from_external_file = TeradataOperator(
        task_id="create_table_from_external_file",
        sql="create_table.sql",
        dag=dag,
    )
    # [END teradata_operator_howto_guide_create_table_from_external_file]
    # [START teradata_operator_howto_guide_populate_table]
    populate_table = TeradataOperator(
        task_id="populate_table",
        sql=r"""
        INSERT INTO Users (username, description)
            VALUES ( 'Danny', 'Musician');
        INSERT INTO Users (username, description)
            VALUES ( 'Simone', 'Chef');
        INSERT INTO Users (username, description)
            VALUES ( 'Lily', 'Florist');
        INSERT INTO Users (username, description)
            VALUES ( 'Tim', 'Pet shop owner');
        """,
    )
    # [END teradata_operator_howto_guide_populate_table]
    # [START teradata_operator_howto_guide_get_all_countries]
    get_all_countries = TeradataOperator(
        task_id="get_all_countries",
        sql=r"SELECT * FROM Country;",
    )
    # [END teradata_operator_howto_guide_get_all_countries]
    # [START teradata_operator_howto_guide_params_passing_get_query]
    get_countries_from_continent = TeradataOperator(
        task_id="get_countries_from_continent",
        sql=r"SELECT * FROM Country WHERE {{ params.column }}='{{ params.value }}';",
        params={"column": "continent", "value": "Asia"},
    )
    # [END teradata_operator_howto_guide_params_passing_get_query]
    # [START teradata_operator_howto_guide_drop_country_table]
    drop_country_table = TeradataOperator(
        task_id="drop_country_table",
        sql=r"DROP TABLE Country;",
        dag=dag,
    )
    # [END teradata_operator_howto_guide_drop_country_table]
    # [START teradata_operator_howto_guide_drop_users_table]
    drop_users_table = TeradataOperator(
        task_id="drop_users_table",
        sql=r"DROP TABLE Users;",
        dag=dag,
    )
    # [END teradata_operator_howto_guide_drop_users_table]
    # [START teradata_operator_howto_guide_create_schema]
    create_schema = TeradataOperator(
        task_id="create_schema",
        sql=r"CREATE DATABASE airflow_temp AS PERM=10e6;",
    )
    # [END teradata_operator_howto_guide_create_schema]
    # [START teradata_operator_howto_guide_create_table_with_schema]
    create_table_with_schema = TeradataOperator(
        task_id="create_table_with_schema",
        sql=r"""
        CREATE TABLE schema_table (
           country_id INTEGER,
           name CHAR(25),
           continent CHAR(25)
        );
        """,
        schema="airflow_temp",
    )
    # [END teradata_operator_howto_guide_create_table_with_schema]
    # [START teradata_operator_howto_guide_drop_schema_table]
    drop_schema_table = TeradataOperator(
        task_id="drop_schema_table",
        sql=r"DROP TABLE schema_table;",
        dag=dag,
        schema="airflow_temp",
    )
    # [END teradata_operator_howto_guide_drop_schema_table]
    # [START teradata_operator_howto_guide_drop_schema]
    drop_schema = TeradataOperator(
        task_id="drop_schema",
        sql=r"DROP DATABASE airflow_temp;",
        dag=dag,
    )

    # [END teradata_operator_howto_guide_drop_schema]
    (
        create_table
        >> create_table_from_external_file
        >> populate_table
        >> get_all_countries
        >> get_countries_from_continent
        >> drop_country_table
        >> drop_users_table
        >> create_schema
        >> create_table_with_schema
        >> drop_schema_table
        >> drop_schema
    )

    # [END teradata_operator_howto_guide]

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
