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
Example DAG using TrinoOperator to query with Trino.
"""

from __future__ import annotations

from datetime import datetime

from airflow import models
from airflow.providers.trino.operators.trino import TrinoOperator

SCHEMA = "hive.cities"
TABLE = "city"
TABLE1 = "city1"
TABLE2 = "city2"

# [START howto_operator_trino]

with models.DAG(
    dag_id="example_trino",
    schedule="@once",  # Override to match your needs
    start_date=datetime(2025, 2, 17),
    catchup=False,
    tags=["example"],
) as dag:

    trino_create_schema = TrinoOperator(
        task_id="trino_create_schema",
        sql=f" CREATE SCHEMA IF NOT EXISTS {SCHEMA} WITH (location = 's3://example-bucket/cities/') ",
    )

    trino_create_table = TrinoOperator(
        task_id="trino_create_table",
        sql=f" CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} ( cityid bigint, cityname varchar) ",
    )

    trino_insert = TrinoOperator(
        task_id="trino_insert",
        sql=f" INSERT INTO {SCHEMA}.{TABLE} VALUES (1, 'San Francisco') "
    )

    trino_multiple_queries = TrinoOperator(
        task_id="trino_multiple_queries",
        sql=[
            f" CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE1}(cityid bigint,cityname varchar) ",
            f" INSERT INTO {SCHEMA}.{TABLE1} VALUES (2, 'San Jose') ",
            f" CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE2}(cityid bigint,cityname varchar) ",
            f" INSERT INTO {SCHEMA}.{TABLE2} VALUES (3, 'San Diego') "
        ]
    )

    trino_templated_query = TrinoOperator(
        task_id="trino_templated_query",
        sql="SELECT * FROM {{ params.SCHEMA }}.{{ params.TABLE }}",
        params={"SCHEMA": SCHEMA, "TABLE": TABLE1}
    )

    trino_parameterized_query = TrinoOperator(
        task_id="trino_parameterized_query",
        sql=f"SELECT * FROM {SCHEMA}.{TABLE2} WHERE cityname = ?",
        parameters=("San Diego",)
    )

    (
        trino_create_schema
        >> trino_create_table
        >> trino_insert
        >> trino_multiple_queries
        >> trino_templated_query
        >> trino_parameterized_query
    )

    # [END howto_operator_trino]


from tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
