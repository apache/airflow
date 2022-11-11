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
Example use of Snowflake Snowpark related decorator.
"""
from __future__ import annotations

import sys

import pytest

if not sys.version_info[0:2] == (3, 8):
    pytest.skip("unsupported python version", allow_module_level=True)
from datetime import datetime
from random import uniform

import snowflake.snowpark
from snowflake.snowpark.functions import col, count, lit, random as spRandom, sproc, uniform as spUniform
from snowflake.snowpark.types import FloatType

from airflow import DAG, AirflowException
from airflow.decorators import task

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
DAG_ID = "example_snowflake_snowpark"

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval="@once",
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    catchup=False,
) as dag:

    # [START howto_decorator_snowpark]
    @task.snowpark
    def calculate_pi_sql(point_count, snowpark_session: snowflake.snowpark.Session):
        return float(
            (
                snowpark_session.range(0, point_count)
                .select(
                    spUniform(lit(-1.0).cast(FloatType()), lit(1.0).cast(FloatType()), spRandom()).alias("x"),
                    spUniform(lit(-1.0).cast(FloatType()), lit(1.0).cast(FloatType()), spRandom()).alias("y"),
                )
                .filter(((col("x") * col("x")) + (col("y") * col("y"))) < 1)
                .select([(4 * count(lit("x"))) / point_count])
            ).first()[0]
        )

    sql_result = calculate_pi_sql(10000)
    # [END howto_decorator_snowpark]

    # [START howto_decorator_snowpark_sproc]
    @task.snowpark
    def calculate_pi_sproc(point_count, snowpark_session: snowflake.snowpark.Session):
        snowpark_session.add_packages("snowflake-snowpark-python")

        @sproc(session=snowpark_session)
        def compute(session: snowflake.snowpark.Session, point_count: int) -> float:
            result = 0
            for _ in range(point_count):
                x = uniform(-1, 1)
                y = uniform(-1, 1)
                if x * x + y * y < 1:
                    result += 1
            return 4 * result / point_count

        return compute(point_count, session=snowpark_session)

    sproc_result = calculate_pi_sproc(10000)
    # [END howto_decorator_snowpark_sproc]

    @task
    def assert_result(sql_r, sproc_r):
        if not (3 < sql_r < 4):
            raise AirflowException(f"Invalid result. Expect PI. Current value: {sql_r}")
        if not (3 < sproc_r < 4):
            raise AirflowException(f"Invalid result. Expect PI. Current value: {sproc_r}")

    assert_result(sql_result, sproc_result)

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
