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
Example Airflow DAG to show usage of TeradataOperator with SSL teradata connection.

This DAG assumes Airflow Connection with connection id `teradata_ssl_default` already exists in locally. It
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


# create_table_teradata, insert_teradata_task, create_table_teradata_from_external_file,
# populate_user_table get_all_countries, get_all_description, get_countries_from_continent,
# drop_table_teradata_task, drop_users_table_teradata_task are examples of tasks created by instantiating
# the Teradata Operator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_ssl_teradata"
CONN_ID = "teradata_ssl_default"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
    default_args={"teradata_conn_id": CONN_ID},
) as dag:
    # [START teradata_operator_howto_guide_create_country_table]
    create_country_table = TeradataOperator(
        task_id="create_country_table",
        sql=r"""
        CREATE TABLE SSL_Country (
            country_id INTEGER,
            name CHAR(25),
            continent CHAR(25)
        );
        """,
        trigger_rule="all_done",
    )
    # [END teradata_operator_howto_guide_create_country_table]
    # [START teradata_operator_howto_guide_populate_country_table]
    populate_country_table = TeradataOperator(
        task_id="populate_country_table",
        sql=r"""
                INSERT INTO SSL_Country VALUES ( 1, 'India', 'Asia');
                INSERT INTO SSL_Country VALUES ( 2, 'Germany', 'Europe');
                INSERT INTO SSL_Country VALUES ( 3, 'Argentina', 'South America');
                INSERT INTO SSL_Country VALUES ( 4, 'Ghana', 'Africa');
                """,
    )
    # [END teradata_operator_howto_guide_populate_country_table]
    # [START teradata_operator_howto_guide_create_users_table_from_external_file]
    create_users_table_from_external_file = TeradataOperator(
        task_id="create_users_table_from_external_file",
        sql="create_ssl_table.sql",
        dag=dag,
    )
    # [END teradata_operator_howto_guide_create_users_table_from_external_file]
    # [START teradata_operator_howto_guide_get_all_countries]
    get_all_countries = TeradataOperator(
        task_id="get_all_countries",
        sql=r"SELECT * FROM SSL_Country;",
    )
    # [END teradata_operator_howto_guide_get_all_countries]
    # [START teradata_operator_howto_guide_params_passing_get_query]
    get_countries_from_continent = TeradataOperator(
        task_id="get_countries_from_continent",
        sql=r"SELECT * FROM SSL_Country where {{ params.column }}='{{ params.value }}';",
        params={"column": "continent", "value": "Asia"},
    )
    # [END teradata_operator_howto_guide_params_passing_get_query]
    # [START teradata_operator_howto_guide_drop_country_table]
    drop_country_table = TeradataOperator(
        task_id="drop_country_table", sql=r"DROP TABLE SSL_Country;", dag=dag
    )
    # [END teradata_operator_howto_guide_drop_country_table]
    # [START teradata_operator_howto_guide_drop_users_table]
    drop_users_table = TeradataOperator(
        task_id="drop_users_table", sql=r"DROP TABLE SSL_Users;", dag=dag
    )
    # [END teradata_operator_howto_guide_drop_users_table]

    (
        create_country_table
        >> populate_country_table
        >> create_users_table_from_external_file
        >> get_all_countries
        >> get_countries_from_continent
        >> drop_country_table
        >> drop_users_table
    )

    # [END teradata_operator_howto_guide]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
