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
Example use of Teradata related operators.
"""
from __future__ import annotations

import os
import pytest
from datetime import datetime

from airflow import DAG
try:
    from airflow.providers.teradata.hooks.teradata import TeradataHook
    from airflow.providers.teradata.operators.teradata import TeradataOperator
except ImportError:
    pytest.skip("TERADATA provider not available", allow_module_level=True)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_teradata"

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={"teradata_conn_id": "teradata_conn_id"},
    tags=["example"],
    catchup=False,
) as dag:
    
    # [START howto_operator_teradata]

    # Example of creating a task to create a table in Teradata

    create_table_teradata_task = TeradataOperator(
        task_id="create_country_table",
        teradata_conn_id="airflow_teradata",
        sql=r"""
        CREATE TABLE Country (
            country_id INTEGER,
            name CHAR(25),
            continent CHAR(25)
        );
        """,
        dag=dag,
    )

    # [END howto_operator_teradata]

    # [START teradata_operator_howto_guide_insert_country_table]
    insert_teradata_task = TeradataOperator(
        task_id="insert_teradata_task",
        teradata_conn_id="airflow_teradata",
        sql=r"""
                INSERT INTO Country VALUES ( 1, 'India', 'Asia');
                INSERT INTO Country VALUES ( 2, 'Germany', 'Europe');
                INSERT INTO Country VALUES ( 3, 'Argentina', 'South America');
                INSERT INTO Country VALUES ( 4, 'Ghana', 'Africa');
                """,
    )
    # [END teradata_operator_howto_guide_populate_country_table]

    

    # [START howto_operator_teradata]

    # [START teradata_operator_howto_guide_create_table_teradata_from_external_file]
    # Example of creating a task that calls an sql command from an external file.
    create_table_teradata_from_external_file = TeradataOperator(
        task_id="create_table_from_external_file",
        teradata_conn_id="airflow_teradata",
        sql="create_table.sql",
        dag=dag,
    )
    # [END teradata_operator_howto_guide_create_table_teradata_from_external_file]

    # [START teradata_operator_howto_guide_populate_user_table]
    populate_user_table = TeradataOperator(
        task_id="populate_user_table",
        teradata_conn_id="airflow_teradata",
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
    # [END teradata_operator_howto_guide_populate_user_table]

    # [START teradata_operator_howto_guide_get_all_countries]
    get_all_countries = TeradataOperator(
        task_id="get_all_countries",
        teradata_conn_id="airflow_teradata",
        sql=r"""SELECT * FROM Country;""",
    )
    # [END teradata_operator_howto_guide_get_all_countries]

    # [START teradata_operator_howto_guide_get_all_description]
    get_all_description = TeradataOperator(
        task_id="get_all_description",
        teradata_conn_id="airflow_teradata",
        sql=r"""SELECT description FROM Users;""",
    )
    # [END teradata_operator_howto_guide_get_all_description]

    # [START teradata_operator_howto_guide_params_passing_get_query]
    get_countries_from_continent = TeradataOperator(
        task_id="get_countries_from_continent",
        teradata_conn_id="airflow_teradata",
        sql=r"""SELECT * FROM Country where {{ params.column }}='{{ params.value }}';""",
        params={"column": "CONVERT(VARCHAR, continent)", "value": "Asia"},
    )
    # [END teradata_operator_howto_guide_params_passing_get_query]
    

    drop_table_teradata_task = TeradataOperator(
        task_id="drop_table_teradata", sql=r"""DROP TABLE Country;""", dag=dag
    )

    (
        create_table_teradata_task
        >> insert_teradata_task
        >> create_table_teradata_from_external_file
        >> populate_user_table
        >> get_all_countries
        >> get_all_description
        >> get_countries_from_continent
        >> drop_table_teradata_task
    )  

    # [END howto_operator_teradata]

    from tests.system.utils.watcher import watcher
    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
    

    

